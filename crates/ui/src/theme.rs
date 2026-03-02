pub const THEME_CSS: &str = r#"
:root {
    --bg: #ffffff;
    --bg-surface: #ffffff;
    --bg-surface-alt: #f8fafc;
    --text-primary: #0f172a;
    --text-secondary: #64748b;
    --text-muted: #94a3b8;
    --border: #e2e8f0;
    --border-light: #f1f5f9;
    --accent: #2563eb;
    --success: #16a34a;
    --error: #ef4444;
    --error-bg: #fef2f2;
    --error-border: #fecaca;
    --input-bg: #ffffff;
}
[data-theme="dark"] {
    --bg: #0f172a;
    --bg-surface: #1e293b;
    --bg-surface-alt: #1e293b;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --border: #334155;
    --border-light: #1e293b;
    --accent: #3b82f6;
    --success: #22c55e;
    --error: #f87171;
    --error-bg: #450a0a;
    --error-border: #7f1d1d;
    --input-bg: #1e293b;
}
body { margin: 0; }
"#;
