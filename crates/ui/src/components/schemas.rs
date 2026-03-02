use dioxus::prelude::*;
use crate::grpc;

const PAGE_SIZE: usize = 10;

// ── property model ────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq)]
struct PropertyRow {
    name:        String,
    type_:       String,
    description: String,
    required:    bool,
}

impl PropertyRow {
    fn new() -> Self {
        Self {
            name:        String::new(),
            type_:       "string".to_string(),
            description: String::new(),
            required:    false,
        }
    }
}

/// Serialize a property list into a JSON Schema string.
fn rows_to_schema(description: &str, rows: &[PropertyRow]) -> String {
    use serde_json::json;
    let mut properties = serde_json::Map::new();
    let mut required_names: Vec<String> = Vec::new();

    for row in rows {
        let name = row.name.trim();
        if name.is_empty() { continue; }
        let mut prop = serde_json::Map::new();
        prop.insert("type".into(), json!(row.type_));
        if !row.description.is_empty() {
            prop.insert("description".into(), json!(row.description));
        }
        properties.insert(name.to_string(), serde_json::Value::Object(prop));
        if row.required { required_names.push(name.to_string()); }
    }

    let mut schema = serde_json::Map::new();
    schema.insert("type".into(), json!("object"));
    if !description.trim().is_empty() {
        schema.insert("description".into(), json!(description.trim()));
    }
    schema.insert("properties".into(), serde_json::Value::Object(properties));
    if !required_names.is_empty() {
        schema.insert("required".into(), json!(required_names));
    }
    serde_json::to_string_pretty(&serde_json::Value::Object(schema)).unwrap()
}

/// Parse a JSON Schema string back into a property list (best-effort).
fn schema_to_rows(s: &str) -> Vec<PropertyRow> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else { return vec![]; };
    let Some(props) = v.get("properties").and_then(|p| p.as_object()) else { return vec![]; };
    let required: Vec<&str> = v.get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    props.iter().map(|(name, prop)| PropertyRow {
        name:        name.clone(),
        type_:       prop.get("type").and_then(|t| t.as_str()).unwrap_or("string").to_string(),
        description: prop.get("description").and_then(|d| d.as_str()).unwrap_or("").to_string(),
        required:    required.contains(&name.as_str()),
    }).collect()
}

/// Extract the top-level description from a JSON Schema string.
fn schema_description(s: &str) -> String {
    serde_json::from_str::<serde_json::Value>(s)
        .ok()
        .and_then(|v| v.get("description").and_then(|d| d.as_str()).map(str::to_owned))
        .unwrap_or_default()
}

// ── view state ────────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq)]
enum SchemaView {
    List,
    New,
    Detail(String),
    Edit { name: String, definition: String },
}

// ── top-level page ────────────────────────────────────────────────────────────

#[component]
pub fn SchemasPage() -> Element {
    let view = use_signal(|| SchemaView::List);
    let current = view.read().clone();
    match current {
        SchemaView::List                      => rsx! { SchemaList { view } },
        SchemaView::New                       => rsx! { SchemaNew  { view } },
        SchemaView::Detail(name)              => rsx! { SchemaDetail { name, view } },
        SchemaView::Edit { name, definition } => rsx! { SchemaEdit { name, definition, view } },
    }
}

// ── list ──────────────────────────────────────────────────────────────────────

#[component]
fn SchemaList(mut view: Signal<SchemaView>) -> Element {
    let mut page = use_signal(|| 0_usize);
    let schemas = use_resource(|| async { grpc::list_schemas().await });

    match &*schemas.read_unchecked() {
        None => rsx! {
            div { style: "padding:3rem; text-align:center; color:#94a3b8;",
                "Loading schemas…"
            }
        },
        Some(Err(e)) => rsx! {
            div { style: "padding:1rem; color:#ef4444; background:#fef2f2; \
                           border-radius:6px; border:1px solid #fecaca;",
                "Could not load schemas: {e}"
            }
        },
        Some(Ok(all)) => {
            let total       = all.len();
            let pg          = *page.read();
            let total_pages = if total == 0 { 1 } else { (total + PAGE_SIZE - 1) / PAGE_SIZE };
            let start       = pg * PAGE_SIZE;
            let end         = (start + PAGE_SIZE).min(total);
            let visible: Vec<String> = all[start..end].to_vec();

            rsx! {
                div { style: "display:flex; align-items:center; \
                               justify-content:space-between; margin-bottom:1rem;",
                    h2 { style: "margin:0; font-size:1.1rem; color:#1e293b;",
                        "Schemas "
                        span { style: "font-weight:400; color:#94a3b8;", "({total})" }
                    }
                    button {
                        style: BTN_PRIMARY,
                        onclick: move |_| view.set(SchemaView::New),
                        "+ New Schema"
                    }
                }

                div { style: "border:1px solid #e2e8f0; border-radius:8px; \
                               overflow:hidden; background:#fff;",
                    if visible.is_empty() {
                        div { style: "padding:3rem; text-align:center; color:#94a3b8;",
                            "No schemas registered yet."
                        }
                    }
                    for (i, name) in visible.into_iter().enumerate() {
                        SchemaRow { key: "{i}", name, view }
                    }
                }

                if total_pages > 1 {
                    div { style: "display:flex; align-items:center; \
                                   justify-content:center; gap:1rem; margin-top:1rem;",
                        button {
                            style: BTN_OUTLINE,
                            disabled: pg == 0,
                            onclick: move |_| page.set(pg.saturating_sub(1)),
                            "← Prev"
                        }
                        span { style: "color:#64748b; font-size:0.9rem;",
                            "Page {pg + 1} of {total_pages}"
                        }
                        button {
                            style: BTN_OUTLINE,
                            disabled: pg + 1 >= total_pages,
                            onclick: move |_| page.set((pg + 1).min(total_pages - 1)),
                            "Next →"
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn SchemaRow(name: String, mut view: Signal<SchemaView>) -> Element {
    rsx! {
        div {
            style: "display:flex; align-items:center; justify-content:space-between; \
                    padding:0.75rem 1rem; border-bottom:1px solid #f1f5f9; cursor:pointer;",
            onclick: { let n = name.clone(); move |_| view.set(SchemaView::Detail(n.clone())) },
            span { style: "font-size:0.95rem; color:#1e293b;", "{name}" }
            span { style: "color:#cbd5e1; font-size:1.1rem;", "›" }
        }
    }
}

// ── new ───────────────────────────────────────────────────────────────────────

#[component]
fn SchemaNew(mut view: Signal<SchemaView>) -> Element {
    let mut name_buf    = use_signal(String::new);
    let mut schema_desc = use_signal(String::new);
    let properties      = use_signal(Vec::<PropertyRow>::new);
    let mut error       = use_signal(|| None::<String>);
    let mut saving      = use_signal(|| false);

    rsx! {
        div { style: HDR_ROW,
            button {
                style: BTN_OUTLINE_SM,
                onclick: move |_| view.set(SchemaView::List),
                "← Cancel"
            }
            h2 { style: HDR_TITLE, "New Schema" }
            button {
                style: BTN_PRIMARY,
                disabled: *saving.read(),
                onclick: move |_| {
                    let name = name_buf.read().trim().to_string();
                    if name.is_empty() {
                        error.set(Some("Name is required.".into()));
                        return;
                    }
                    let desc    = schema_desc.read().clone();
                    let content = rows_to_schema(&desc, &properties.read());
                    saving.set(true);
                    error.set(None);
                    spawn(async move {
                        match grpc::register_schema(name.clone(), content).await {
                            Ok(()) => view.set(SchemaView::Detail(name)),
                            Err(e) => { error.set(Some(e.to_string())); saving.set(false); }
                        }
                    });
                },
                if *saving.read() { "Saving…" } else { "Save" }
            }
        }

        div { style: "display:grid; grid-template-columns:1fr 2fr; gap:1rem; margin-bottom:1.5rem;",
            div {
                label { style: FIELD_LABEL, "Name" }
                input {
                    r#type: "text",
                    style: FIELD_INPUT,
                    placeholder: "e.g. order, user-event",
                    value: "{name_buf}",
                    oninput: move |e| name_buf.set(e.value()),
                }
            }
            div {
                label { style: FIELD_LABEL, "Description" }
                input {
                    r#type: "text",
                    style: FIELD_INPUT,
                    placeholder: "What kind of tuples does this schema describe?",
                    value: "{schema_desc}",
                    oninput: move |e| schema_desc.set(e.value()),
                }
            }
        }

        ErrorBanner { error }
        PropertyList { properties }
    }
}

// ── detail ────────────────────────────────────────────────────────────────────

#[component]
fn SchemaDetail(name: String, mut view: Signal<SchemaView>) -> Element {
    let fetch_name = name.clone();
    let schema = use_resource(move || {
        let n = fetch_name.clone();
        async move { grpc::get_schema(n).await }
    });

    rsx! {
        div { style: HDR_ROW,
            button {
                style: BTN_OUTLINE_SM,
                onclick: move |_| view.set(SchemaView::List),
                "← Back"
            }
            h2 { style: HDR_TITLE, "{name}" }
        }

        match &*schema.read_unchecked() {
            None => rsx! { p { style: "color:#94a3b8;", "Loading…" } },
            Some(Err(e)) => rsx! { p { style: "color:#ef4444;", "Error: {e}" } },
            Some(Ok(definition)) => {
                let rows          = schema_to_rows(definition);
                let desc          = schema_description(definition);
                let pretty        = pretty_json(definition);
                let def_for_edit  = definition.clone();
                let name_for_edit = name.clone();
                rsx! {
                    div { style: "display:flex; justify-content:flex-end; margin-bottom:0.75rem;",
                        button {
                            style: BTN_PRIMARY,
                            onclick: move |_| view.set(SchemaView::Edit {
                                name: name_for_edit.clone(),
                                definition: def_for_edit.clone(),
                            }),
                            "Edit"
                        }
                    }

                    if !desc.is_empty() {
                        p { style: "margin:0 0 1rem; color:#64748b; font-size:0.9rem; \
                                    font-style:italic;",
                            "{desc}"
                        }
                    }

                    if rows.is_empty() {
                        div { style: "padding:2rem; text-align:center; color:#94a3b8; \
                                       border:1px dashed #e2e8f0; border-radius:8px; \
                                       font-size:0.9rem; margin-bottom:1rem;",
                            "No properties defined."
                        }
                    } else {
                        div { style: "border:1px solid #e2e8f0; border-radius:8px; \
                                       overflow:hidden; margin-bottom:1rem;",
                            table { style: "width:100%; border-collapse:collapse; font-size:0.9rem;",
                                thead {
                                    tr { style: "background:#f8fafc; \
                                                  border-bottom:1px solid #e2e8f0;",
                                        th { style: TH_STYLE, "Name" }
                                        th { style: TH_STYLE, "Type" }
                                        th { style: "{TH_STYLE} text-align:center;", "Required" }
                                        th { style: TH_STYLE, "Description" }
                                    }
                                }
                                tbody {
                                    for (i, row) in rows.iter().enumerate() {
                                        tr {
                                            style: if i % 2 == 0 {
                                                "background:#fff;"
                                            } else {
                                                "background:#f8fafc;"
                                            },
                                            td { style: TD_STYLE,
                                                span { style: "font-family:monospace; \
                                                                font-weight:600; color:#1e293b;",
                                                    "{row.name}"
                                                }
                                            }
                                            td { style: TD_STYLE,
                                                TypeBadge { type_: row.type_.clone() }
                                            }
                                            td { style: "{TD_STYLE} text-align:center;",
                                                if row.required {
                                                    span { style: "color:#16a34a; font-size:1rem;",
                                                        "✓"
                                                    }
                                                } else {
                                                    span { style: "color:#cbd5e1;", "—" }
                                                }
                                            }
                                            td { style: "{TD_STYLE} color:#64748b;",
                                                "{row.description}"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    details { style: "margin-bottom:1rem;",
                        summary { style: "cursor:pointer; color:#64748b; font-size:0.85rem; \
                                          user-select:none; padding:0.4rem 0;",
                            "Raw JSON"
                        }
                        pre { style: "background:#f8fafc; border:1px solid #e2e8f0; \
                                       border-radius:8px; padding:1.25rem; overflow:auto; \
                                       font-size:0.85rem; line-height:1.6; color:#334155; \
                                       white-space:pre-wrap; word-break:break-word; \
                                       margin-top:0.5rem;",
                            "{pretty}"
                        }
                    }
                }
            }
        }
    }
}

// ── edit ──────────────────────────────────────────────────────────────────────

#[component]
fn SchemaEdit(name: String, definition: String, mut view: Signal<SchemaView>) -> Element {
    let mut schema_desc = use_signal(|| schema_description(&definition));
    let properties      = use_signal(|| schema_to_rows(&definition));
    let mut error       = use_signal(|| None::<String>);
    let mut saving      = use_signal(|| false);

    rsx! {
        div { style: HDR_ROW,
            button {
                style: BTN_OUTLINE_SM,
                onclick: { let n = name.clone(); move |_| view.set(SchemaView::Detail(n.clone())) },
                "← Cancel"
            }
            h2 { style: HDR_TITLE, "{name}" }
            button {
                style: BTN_PRIMARY,
                disabled: *saving.read(),
                onclick: {
                    let n = name.clone();
                    move |_| {
                        let save_name = n.clone();
                        let desc      = schema_desc.read().clone();
                        let content   = rows_to_schema(&desc, &properties.read());
                        saving.set(true);
                        error.set(None);
                        spawn(async move {
                            match grpc::register_schema(save_name.clone(), content).await {
                                Ok(()) => view.set(SchemaView::Detail(save_name)),
                                Err(e) => { error.set(Some(e.to_string())); saving.set(false); }
                            }
                        });
                    }
                },
                if *saving.read() { "Saving…" } else { "Save" }
            }
        }

        div { style: "margin-bottom:1.5rem;",
            label { style: FIELD_LABEL, "Description" }
            input {
                r#type: "text",
                style: FIELD_INPUT,
                placeholder: "What kind of tuples does this schema describe?",
                value: "{schema_desc}",
                oninput: move |e| schema_desc.set(e.value()),
            }
        }

        ErrorBanner { error }
        PropertyList { properties }
    }
}

// ── property list ─────────────────────────────────────────────────────────────

#[component]
fn PropertyList(mut properties: Signal<Vec<PropertyRow>>) -> Element {
    let len = properties.read().len();

    rsx! {
        div { style: "display:flex; align-items:center; \
                       justify-content:space-between; margin-bottom:0.75rem;",
            h3 { style: "margin:0; font-size:0.95rem; font-weight:600; color:#374151;",
                "Properties"
            }
            button {
                style: BTN_OUTLINE,
                onclick: move |_| properties.write().push(PropertyRow::new()),
                "+ Add Property"
            }
        }

        if len == 0 {
            div { style: "padding:2rem; text-align:center; color:#94a3b8; \
                           border:1px dashed #e2e8f0; border-radius:8px; font-size:0.9rem;",
                "No properties yet. Click \"+ Add Property\" to add one."
            }
        }

        for i in 0..len {
            PropertyRowEditor { key: "{i}", idx: i, properties }
        }
    }
}

// ── property row editor ───────────────────────────────────────────────────────

#[component]
fn PropertyRowEditor(idx: usize, mut properties: Signal<Vec<PropertyRow>>) -> Element {
    let prop = properties.read().get(idx).cloned().unwrap_or_else(PropertyRow::new);
    // New rows (empty name) start expanded; existing rows start collapsed.
    let mut expanded = use_signal(|| prop.name.is_empty());

    const TYPES: [&str; 6] = ["string", "number", "integer", "boolean", "array", "object"];

    if !*expanded.read() {
        // ── compact view ──────────────────────────────────────────────────────
        let chars: Vec<char> = prop.description.chars().collect();
        let desc_preview = if chars.len() > 60 {
            format!("{}…", chars[..60].iter().collect::<String>())
        } else {
            prop.description.clone()
        };
        rsx! {
            div {
                style: "display:flex; align-items:center; gap:0.75rem; \
                         border:1px solid #e2e8f0; border-radius:8px; \
                         padding:0.6rem 1rem; background:#fff; \
                         margin-bottom:0.5rem; cursor:pointer;",
                onclick: move |_| expanded.set(true),

                span { style: "font-weight:600; color:#1e293b; font-size:0.9rem; \
                                min-width:8rem; flex-shrink:0;",
                    "{prop.name}"
                }
                TypeBadge { type_: prop.type_.clone() }
                if prop.required {
                    span { style: "font-size:0.75rem; font-weight:500; color:#16a34a; \
                                    background:#f0fdf4; border:1px solid #bbf7d0; \
                                    border-radius:4px; padding:0.1rem 0.4rem; flex-shrink:0;",
                        "required"
                    }
                }
                span { style: "flex:1; color:#94a3b8; font-size:0.85rem; overflow:hidden; \
                                text-overflow:ellipsis; white-space:nowrap;",
                    "{desc_preview}"
                }
                span { style: "color:#94a3b8; font-size:0.8rem; flex-shrink:0;", "▾" }
            }
        }
    } else {
        // ── expanded view ─────────────────────────────────────────────────────
        rsx! {
            div { style: "border:2px solid #2563eb; border-radius:8px; padding:1rem; \
                           background:#fff; margin-bottom:0.75rem;",

                // ── header: collapse + delete ──
                div { style: "display:flex; justify-content:space-between; \
                               align-items:center; margin-bottom:0.75rem;",
                    button {
                        style: BTN_OUTLINE_SM,
                        onclick: move |_| expanded.set(false),
                        "▴ Collapse"
                    }
                    button {
                        style: BTN_DANGER_SM,
                        onclick: move |_| {
                            let mut v = properties.write();
                            if idx < v.len() { v.remove(idx); }
                        },
                        "× Delete"
                    }
                }

                // ── name / type / required ──
                div { style: "display:grid; grid-template-columns:1fr auto auto; \
                               gap:0.75rem; align-items:end; margin-bottom:0.75rem;",

                    div {
                        label { style: FIELD_LABEL, "Name" }
                        input {
                            r#type: "text",
                            style: FIELD_INPUT,
                            placeholder: "property_name",
                            value: "{prop.name}",
                            oninput: move |e| {
                                if let Some(p) = properties.write().get_mut(idx) {
                                    p.name = e.value();
                                }
                            },
                        }
                    }

                    div {
                        label { style: FIELD_LABEL, "Type" }
                        select {
                            style: FIELD_SELECT,
                            onchange: move |e| {
                                if let Some(p) = properties.write().get_mut(idx) {
                                    p.type_ = e.value();
                                }
                            },
                            for t in TYPES {
                                option { value: "{t}", selected: prop.type_ == t, "{t}" }
                            }
                        }
                    }

                    div { style: "display:flex; flex-direction:column; align-items:center;",
                        label { style: FIELD_LABEL, "Required" }
                        input {
                            r#type: "checkbox",
                            style: "width:1.1rem; height:1.1rem; cursor:pointer; \
                                    margin-top:0.45rem;",
                            checked: prop.required,
                            onclick: move |_| {
                                if let Some(p) = properties.write().get_mut(idx) {
                                    p.required = !p.required;
                                }
                            },
                        }
                    }
                }

                // ── description ──
                label { style: FIELD_LABEL, "Description" }
                textarea {
                    style: "width:100%; padding:0.5rem 0.75rem; border:1px solid #e2e8f0; \
                            border-radius:6px; font-size:0.875rem; line-height:1.5; \
                            color:#374151; resize:vertical; min-height:3.5rem; \
                            box-sizing:border-box; font-family:inherit;",
                    placeholder: "Describe this property (used by AI agents)…",
                    value: "{prop.description}",
                    oninput: move |e| {
                        if let Some(p) = properties.write().get_mut(idx) {
                            p.description = e.value();
                        }
                    },
                }
            }
        }
    }
}

// ── type badge ────────────────────────────────────────────────────────────────

#[component]
fn TypeBadge(type_: String) -> Element {
    let (bg, fg) = match type_.as_str() {
        "string"             => ("#eff6ff", "#1d4ed8"),
        "number" | "integer" => ("#fff7ed", "#c2410c"),
        "boolean"            => ("#f0fdf4", "#15803d"),
        "array"              => ("#faf5ff", "#7e22ce"),
        "object"             => ("#f8fafc", "#475569"),
        _                    => ("#f1f5f9", "#64748b"),
    };
    rsx! {
        span { style: "font-size:0.75rem; font-weight:500; padding:0.15rem 0.5rem; \
                        border-radius:4px; background:{bg}; color:{fg}; flex-shrink:0;",
            "{type_}"
        }
    }
}

// ── shared sub-components ─────────────────────────────────────────────────────

#[component]
fn ErrorBanner(error: Signal<Option<String>>) -> Element {
    match &*error.read_unchecked() {
        None => rsx! {},
        Some(msg) => rsx! {
            div { style: "padding:0.75rem 1rem; background:#fef2f2; border:1px solid #fecaca; \
                           border-radius:6px; color:#ef4444; margin-bottom:1rem; font-size:0.9rem;",
                "{msg}"
            }
        },
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn pretty_json(s: &str) -> String {
    serde_json::from_str::<serde_json::Value>(s)
        .map(|v| serde_json::to_string_pretty(&v).unwrap())
        .unwrap_or_else(|_| s.to_owned())
}

// ── style constants ───────────────────────────────────────────────────────────

const HDR_ROW: &str =
    "display:flex; align-items:center; gap:0.75rem; margin-bottom:1.5rem;";
const HDR_TITLE: &str =
    "flex:1; margin:0; font-size:1.1rem; color:#1e293b;";

const FIELD_LABEL: &str =
    "display:block; font-size:0.8rem; font-weight:500; color:#6b7280; margin-bottom:0.3rem;";
const FIELD_INPUT: &str =
    "width:100%; padding:0.4rem 0.65rem; border:1px solid #e2e8f0; border-radius:6px; \
     font-size:0.9rem; color:#1e293b; box-sizing:border-box;";
const FIELD_SELECT: &str =
    "padding:0.4rem 0.65rem; border:1px solid #e2e8f0; border-radius:6px; \
     font-size:0.9rem; color:#1e293b; background:#fff; cursor:pointer;";

const BTN_PRIMARY: &str =
    "padding:0.4rem 1rem; border:none; border-radius:6px; \
     background:#2563eb; color:#fff; cursor:pointer; font-size:0.9rem;";
const BTN_OUTLINE: &str =
    "padding:0.4rem 0.8rem; border:1px solid #e2e8f0; border-radius:6px; \
     background:#fff; cursor:pointer; color:#374151; font-size:0.9rem;";
const BTN_OUTLINE_SM: &str =
    "padding:0.35rem 0.75rem; border:1px solid #e2e8f0; border-radius:6px; \
     background:#fff; cursor:pointer; color:#64748b; font-size:0.85rem;";
const BTN_DANGER_SM: &str =
    "padding:0.35rem 0.6rem; border:1px solid #fecaca; border-radius:6px; \
     background:#fff; cursor:pointer; color:#ef4444; font-size:0.9rem;";

const TH_STYLE: &str =
    "padding:0.6rem 1rem; text-align:left; font-size:0.8rem; font-weight:600; \
     color:#64748b; text-transform:uppercase; letter-spacing:0.05em;";
const TD_STYLE: &str =
    "padding:0.6rem 1rem; border-top:1px solid #f1f5f9; vertical-align:middle;";
