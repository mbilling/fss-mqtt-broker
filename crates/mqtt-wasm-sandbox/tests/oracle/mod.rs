//! Reads `tests/emqx-jq-oracle.txt` (what EMQX's jq NIF answered, written by
//! `jq-module/emqx-oracle.py`) and compares an answer of the jq module with it.
//! Shared by `tests/jq_module.rs` and `examples/oracle.rs`.

use base64::Engine as _;
use mqtt_wasm_sandbox::jq::JqError;
use mqtt_wasm_sandbox::Arg;

/// What EMQX answered.
#[derive(Debug)]
pub enum Expected {
    /// jq's outputs, each as the text jq printed.
    Ok(Vec<Vec<u8>>),
    /// The NIF's error tag and message.
    Error { tag: String, message: Vec<u8> },
    /// The call took the EMQX node down.
    NodeDown,
}

/// One program, one input, EMQX's answer.
#[derive(Debug)]
pub struct Case {
    pub program: Vec<u8>,
    pub input: Vec<u8>,
    /// The input as the oracle file names it (`@nest:N` for a generated one).
    pub input_name: String,
    pub expected: Expected,
}

impl Case {
    /// The input as EMQX passed it: a binary, read by jq's own parser.
    pub fn input_arg(&self) -> Arg<'_> {
        Arg::Binary(&self.input)
    }

    /// The case on one line, for a report.
    pub fn describe(&self) -> String {
        let input = if self.input_name.starts_with('@') {
            self.input_name.clone()
        } else {
            String::from_utf8_lossy(&self.input).into_owned()
        };
        format!(
            "jq({:?}, {input:?})",
            String::from_utf8_lossy(&self.program)
        )
    }
}

fn b64(field: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(field)
        .expect("base64 in the oracle file")
}

fn input(field: &str) -> Vec<u8> {
    let brackets = |n: &str, close: bool| {
        let n: usize = n.parse().expect("a count");
        let mut text = vec![b'['; n];
        if close {
            text.extend(std::iter::repeat_n(b']', n));
        }
        text
    };
    if let Some(n) = field.strip_prefix("@nest:") {
        brackets(n, true)
    } else if let Some(n) = field.strip_prefix("@open:") {
        brackets(n, false)
    } else {
        b64(field)
    }
}

/// Every case of the oracle file.
pub fn parse(text: &str) -> Vec<Case> {
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let fields: Vec<&str> = line.split(' ').collect();
            let expected = match fields[2] {
                "ok" => Expected::Ok(fields[3..].iter().map(|f| b64(f)).collect()),
                "error" => Expected::Error {
                    tag: fields[3].to_owned(),
                    message: b64(fields[4]),
                },
                "nodedown" => Expected::NodeDown,
                other => panic!("unknown verdict {other:?} in the oracle file"),
            };
            Case {
                program: b64(fields[0]),
                input: input(fields[1]),
                input_name: fields[1].to_owned(),
                expected,
            }
        })
        .collect()
}

/// The environment the module is given for the comparison: the two variables the
/// cases look at, present as they are in the EMQX container.
pub fn environment() -> Vec<Vec<u8>> {
    vec![
        b"PATH=/usr/local/bin:/usr/bin:/bin".to_vec(),
        b"HOME=/opt/emqx".to_vec(),
    ]
}

/// How the module's answer stands against EMQX's.
#[derive(Debug)]
pub enum Verdict {
    Same,
    Differs {
        emqx: String,
        ours: String,
    },
    /// EMQX has no answer: the call took the node down. Ours is whatever it is.
    EmqxWentDown(String),
}

fn show(answer: &Result<Vec<u8>, (String, Vec<u8>)>) -> String {
    let short = |bytes: &[u8]| {
        let text = String::from_utf8_lossy(bytes);
        if text.len() > 300 && std::env::var_os("ORACLE_FULL").is_none() {
            let cut = (0..=300)
                .rev()
                .find(|&i| text.is_char_boundary(i))
                .unwrap_or(0);
            format!("{}… ({} bytes)", &text[..cut], text.len())
        } else {
            text.into_owned()
        }
    };
    match answer {
        Ok(outputs) => short(outputs),
        Err((tag, message)) => format!("{{{tag}, {:?}}}", short(message)),
    }
}

/// Compares the module's answer, byte for byte, with EMQX's.
pub fn compare(case: &Case, got: &Result<Vec<u8>, JqError>) -> Verdict {
    let ours = match got {
        Ok(outputs) => Ok(outputs.clone()),
        Err(e) => Err((e.tag.as_str().to_owned(), e.message.clone())),
    };
    let emqx = match &case.expected {
        Expected::NodeDown => return Verdict::EmqxWentDown(show(&ours)),
        Expected::Ok(outputs) => {
            let mut text = vec![b'['];
            text.extend(outputs.join(&b','));
            text.push(b']');
            Ok(text)
        }
        Expected::Error { tag, message } => Err((tag.clone(), message.clone())),
    };
    if emqx == ours {
        Verdict::Same
    } else {
        Verdict::Differs {
            emqx: show(&emqx),
            ours: show(&ours),
        }
    }
}
