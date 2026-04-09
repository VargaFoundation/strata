//! Startup banner.

pub fn print() {
    let version = env!("CARGO_PKG_VERSION");
    eprintln!(
        r#"
  ____  _             _
 / ___|| |_ _ __ __ _| |_ __ _
 \___ \| __| '__/ _` | __/ _` |
  ___) | |_| | | (_| | || (_| |
 |____/ \__|_|  \__,_|\__\__,_|

  The open-source context lake for AI agents
  Version: {version}
"#
    );
}
