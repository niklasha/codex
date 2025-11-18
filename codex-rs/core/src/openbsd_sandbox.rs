#![cfg(target_os = "openbsd")]
//! OpenBSD pledge-based sandbox backend (no unveil).
//! Applies pledge/execpromises in pre_exec; the parent process remains
//! unaffected. Note: some shells (ksh) re-pledge in ways that require
//! wpath/cpath; denied redirections may abort the child before flushing output.

use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::path::PathBuf;

use tokio::process::Child;
use tokio::process::Command;

use crate::protocol::SandboxPolicy;
use crate::spawn::StdioPolicy;
use crate::spawn::spawn_child_async;

#[derive(Clone)]
struct OpenbsdSandboxPlan {
    promises: CString,
    exec_promises: CString,
}

pub async fn spawn_command_under_openbsd_sandbox(
    program: PathBuf,
    args: Vec<String>,
    arg0: Option<&str>,
    cwd: PathBuf,
    sandbox_policy: &SandboxPolicy,
    stdio_policy: StdioPolicy,
    env: HashMap<String, String>,
) -> io::Result<Child> {
    let plan = match build_openbsd_sandbox_plan(sandbox_policy)? {
        Some(plan) => plan,
        None => {
            return spawn_child_async(program, args, arg0, cwd, sandbox_policy, stdio_policy, env)
                .await;
        }
    };

    let mut cmd = Command::new(&program);
    cmd.arg0(arg0.map_or_else(|| program.to_string_lossy().to_string(), String::from));
    cmd.args(args);
    cmd.current_dir(&cwd);
    cmd.env_clear();
    cmd.envs(env);

    match stdio_policy {
        StdioPolicy::RedirectForShellTool => {
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
        }
        StdioPolicy::Inherit => {
            cmd.stdin(std::process::Stdio::inherit())
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit());
        }
    }

    cmd.kill_on_drop(true);

    unsafe {
        cmd.pre_exec(move || {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }

            apply_openbsd_sandbox(&plan)?;
            Ok(())
        });
    }

    cmd.spawn()
}

fn build_openbsd_sandbox_plan(policy: &SandboxPolicy) -> io::Result<Option<OpenbsdSandboxPlan>> {
    match policy {
        SandboxPolicy::DangerFullAccess => Ok(None),
        SandboxPolicy::ReadOnly | SandboxPolicy::WorkspaceWrite { .. } => {
            let (promises, exec_promises) = openbsd_promises(policy);
            Ok(Some(OpenbsdSandboxPlan {
                promises,
                exec_promises,
            }))
        }
    }
}

fn openbsd_promises(policy: &SandboxPolicy) -> (CString, CString) {
    // Base: stdio+read, process mgmt, exec, tty/pty, unix sockets.
    let mut pre_exec = vec!["stdio", "rpath", "getpw", "proc", "exec", "tty", "unix"];

    if let SandboxPolicy::WorkspaceWrite { network_access, .. } = policy {
        pre_exec.extend(["wpath", "cpath", "fattr", "tmppath"]);
        if *network_access {
            pre_exec.extend(["inet", "dns"]);
        }
    }

    // Post-exec promises: drop write-related promises for ReadOnly.
    let mut post_exec = pre_exec.clone();
    if matches!(policy, SandboxPolicy::ReadOnly) {
        post_exec.retain(|p| *p != "wpath" && *p != "cpath" && *p != "fattr");
    }

    let merged = CString::new(pre_exec.join(" ")).expect("promises cstring");
    let exec_merged = CString::new(post_exec.join(" ")).expect("exec_promises cstring");

    (merged, exec_merged)
}

fn apply_openbsd_sandbox(plan: &OpenbsdSandboxPlan) -> io::Result<()> {
    if unsafe { libc::pledge(plan.promises.as_ptr(), plan.exec_promises.as_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
