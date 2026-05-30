# Security Policy

AgentProf is local-first and handles trace data that may contain sensitive inputs, outputs, identifiers, or secrets. Please report security issues privately.

## Reporting A Vulnerability

Open a private security advisory on GitHub or contact the repository owner through GitHub. Please include:

- affected version or commit
- a concise reproduction
- what data could be exposed or modified
- any suggested mitigation

Do not open a public issue for suspected data exposure, secret handling bugs, path traversal, or unsafe report rendering.

## Supported Versions

AgentProf is pre-1.0. Security fixes target the latest main branch until formal release support is defined.

## Security Expectations

- Report artifacts must escape trace-controlled strings.
- Raw inputs and outputs must not be persisted by default.
- Report display paths must stay inside the AgentProf reports directory.
- Generated reports must not require external scripts, fonts, CDNs, or network calls.
