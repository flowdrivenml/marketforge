//! Streams-first administrative UI. Rendering never performs network or engine work.
mod backend;
use crate::{
    client::management::MarketForgeManagementClient,
    management_protocol::{definitions::*, *},
};
pub use backend::{HttpBackend, Intent, Update};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
};
use std::time::{Duration, Instant};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Streams,
    Runtime,
    Integrity,
    Clients,
    Activity,
    Settings,
}
impl Screen {
    fn next(self) -> Self {
        match self {
            Self::Streams => Self::Runtime,
            Self::Runtime => Self::Integrity,
            Self::Integrity => Self::Clients,
            Self::Clients => Self::Activity,
            Self::Activity => Self::Settings,
            Self::Settings => Self::Streams,
        }
    }
}
#[derive(Debug, Clone)]
pub enum Modal {
    Dependencies {
        items: Vec<String>,
        focus: usize,
    },
    Actions,
    CreateType,
    Form {
        kind: String,
        fields: Vec<(String, String)>,
        focus: usize,
    },
    Review(ChangePlan),
    Help,
}
pub struct App {
    pub scroll: u16,
    pub activity: std::collections::VecDeque<ManagementEvent>,
    pub screen: Screen,
    pub overview: Option<Overview>,
    pub selected: Option<String>,
    pub filter: String,
    pub filtering: bool,
    pub details: bool,
    pub detail_tab: usize,
    pub modal: Option<Modal>,
    pub connected: bool,
    pub updated: Option<Instant>,
    pub notice: String,
    pub quit: bool,
    pub capabilities: Option<Capabilities>,
    pub settings: Vec<SettingView>,
    pub clients: serde_json::Value,
    pub integrity: serde_json::Value,
    pub leases: Vec<DemandLease>,
    counter: u64,
}
impl Default for App {
    fn default() -> Self {
        Self {
            scroll: 0,
            activity: std::collections::VecDeque::new(),
            screen: Screen::Streams,
            overview: None,
            selected: None,
            filter: String::new(),
            filtering: false,
            details: false,
            detail_tab: 0,
            modal: None,
            connected: false,
            updated: None,
            notice: "Connecting; viewing acquires no market demand".into(),
            quit: false,
            capabilities: None,
            settings: vec![],
            clients: serde_json::Value::Null,
            integrity: serde_json::Value::Null,
            leases: vec![],
            counter: 0,
        }
    }
}
pub fn safe_text(text: &str) -> String {
    text.chars()
        .filter(|c| {
            !c.is_control() && !matches!(*c,'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')
        })
        .take(16384)
        .collect()
}
impl App {
    pub fn update(&mut self, update: Update) {
        match update {
            Update::Event(event) => {
                self.activity.push_back(event);
                while self.activity.len() > 128 {
                    self.activity.pop_front();
                }
            }
            Update::Overview(v) => {
                if self
                    .overview
                    .as_ref()
                    .is_some_and(|old| old.daemon_instance != v.daemon_instance)
                {
                    self.activity.clear();
                    self.leases.clear();
                    self.notice =
                        "New daemon instance; old temporary holds were not restored".into();
                }
                let old = self.selected.clone();
                self.overview = Some(v);
                self.connected = true;
                self.updated = Some(Instant::now());
                self.selected = old
                    .filter(|id| self.rows().iter().any(|s| s.stream_id == *id))
                    .or_else(|| self.rows().first().map(|s| s.stream_id.clone()));
            }
            Update::Disconnected => {
                self.connected = false;
                self.notice =
                    "Disconnected: observations stale; mutations disabled; reconnecting".into();
            }
            Update::Error(e) => self.notice = safe_text(&e),
            Update::Plan(p) => self.modal = Some(Modal::Review(p)),
            Update::Operation(o) => {
                self.modal = None;
                self.notice = format!("{}: {}. Readiness is observed separately.", o.id, o.state);
            }
            Update::Capabilities(v) => self.capabilities = Some(v),
            Update::Settings(v) => self.settings = v,
            Update::Clients(v) => self.clients = v,
            Update::Integrity(v) => self.integrity = v,
            Update::Lease(l) => {
                self.notice = format!(
                    "Temporary lease {} expires without renewal; q best-effort releases it",
                    l.id
                );
                self.leases.retain(|v| v.id != l.id);
                self.leases.push(l);
            }
            Update::Released(id) => {
                self.leases.retain(|l| l.id != id);
                self.notice="Released only this UI's hold; other owners or AlwaysOn may keep stream running".into();
            }
        }
    }
    fn rows(&self) -> Vec<&StreamView> {
        self.overview
            .as_ref()
            .map(|v| {
                v.streams
                    .iter()
                    .filter(|s| s.stream_id.contains(&self.filter))
                    .collect()
            })
            .unwrap_or_default()
    }
    fn selected_stream(&self) -> Option<&StreamView> {
        self.overview
            .as_ref()?
            .streams
            .iter()
            .find(|s| Some(&s.stream_id) == self.selected.as_ref())
    }
    fn key(&mut self) -> String {
        self.counter += 1;
        format!("tui-{}-{}", std::process::id(), self.counter)
    }
    pub fn input(&mut self, key: KeyEvent) -> Option<Intent> {
        if self
            .updated
            .is_none_or(|t| t.elapsed() > Duration::from_secs(5))
        {
            self.connected = false;
        }
        if self.filtering {
            match key.code {
                KeyCode::Esc | KeyCode::Enter => self.filtering = false,
                KeyCode::Backspace => {
                    self.filter.pop();
                }
                KeyCode::Char(c) if self.filter.len() < 256 => self.filter.push(c),
                _ => (),
            };
            return None;
        }
        if let Some(modal) = self.modal.take() {
            match modal {
                Modal::Dependencies { items, mut focus } => {
                    match key.code {
                        KeyCode::Esc => return None,
                        KeyCode::Down | KeyCode::Char('j') if !items.is_empty() => {
                            focus = (focus + 1) % items.len()
                        }
                        KeyCode::Up | KeyCode::Char('k') if !items.is_empty() => {
                            focus = (focus + items.len() - 1) % items.len()
                        }
                        KeyCode::Enter => {
                            if let Some(id) = items.get(focus) {
                                self.selected = Some(id.clone());
                                self.detail_tab = 0;
                            }
                            return None;
                        }
                        _ => (),
                    };
                    self.modal = Some(Modal::Dependencies { items, focus });
                    return None;
                }
                Modal::Review(plan) => {
                    if key.code == KeyCode::Enter && self.connected {
                        let key = self.key();
                        return Some(Intent::Apply(plan, key));
                    }
                    if key.code != KeyCode::Esc {
                        self.modal = Some(Modal::Review(plan));
                    }
                    return None;
                }
                Modal::Help => {
                    if key.code != KeyCode::Esc && key.code != KeyCode::Char('?') {
                        self.modal = Some(Modal::Help);
                    }
                    return None;
                }
                Modal::CreateType => {
                    match key.code {
                        KeyCode::Char('1') => self.form("source"),
                        KeyCode::Char('2') => self.form("merge"),
                        KeyCode::Char('3') => self.form("convert"),
                        KeyCode::Esc => (),
                        _ => self.modal = Some(Modal::CreateType),
                    }
                    return None;
                }
                Modal::Actions => {
                    if key.code == KeyCode::Esc {
                        return None;
                    }
                    let Some(stream) = self.selected_stream().cloned() else {
                        return None;
                    };
                    let id = stream.stream_id.clone();
                    let action = match key.code {
                        KeyCode::Char('e') => Some(Action::SetDesired {
                            stream_id: id,
                            expected_revision: stream.definition.revision.to_string(),
                            desired: DesiredStreamState {
                                enabled: !stream.definition.desired.enabled,
                                ..stream.definition.desired
                            },
                            cascade: false,
                        }),
                        KeyCode::Char('a') => Some(Action::SetDesired {
                            stream_id: id,
                            expected_revision: stream.definition.revision.to_string(),
                            desired: DesiredStreamState {
                                activation_policy: if stream.definition.desired.activation_policy
                                    == ActivationPolicy::AlwaysOn
                                {
                                    ActivationPolicy::OnDemand
                                } else {
                                    ActivationPolicy::AlwaysOn
                                },
                                ..stream.definition.desired
                            },
                            cascade: false,
                        }),
                        KeyCode::Char('r') => Some(Action::Restart {
                            stream_id: id,
                            cascade: false,
                        }),
                        KeyCode::Char('R') => Some(Action::Restart {
                            stream_id: id,
                            cascade: true,
                        }),
                        KeyCode::Char('d') => Some(Action::Delete {
                            stream_id: id,
                            expected_revision: stream.definition.revision.to_string(),
                        }),
                        KeyCode::Char('t') => {
                            let key = self.key();
                            return Some(Intent::Acquire(LeaseRequest {
                                stream_id: id,
                                seconds: 60,
                                idempotency_key: key,
                            }));
                        }
                        KeyCode::Char('l') => {
                            if let Some(l) = self.leases.iter().find(|l| l.stream_id == id) {
                                return Some(Intent::Release(l.id.clone()));
                            }
                            None
                        }
                        KeyCode::Char('s') => Some(Action::Shutdown),
                        _ => None,
                    };
                    return action.filter(|_| self.connected).map(Intent::Preview);
                }
                Modal::Form {
                    kind,
                    mut fields,
                    mut focus,
                } => {
                    match key.code {
                        KeyCode::F(2) if kind == "source" => {
                            let filter = fields.get(3).map(|(_, v)| v.clone()).unwrap_or_default();
                            self.modal = Some(Modal::Form {
                                kind,
                                fields,
                                focus,
                            });
                            return Some(Intent::Metadata(filter));
                        }
                        KeyCode::Esc => return None,
                        KeyCode::Tab | KeyCode::Down => focus = (focus + 1) % fields.len(),
                        KeyCode::Up => focus = (focus + fields.len() - 1) % fields.len(),
                        KeyCode::Backspace => {
                            fields[focus].1.pop();
                        }
                        KeyCode::Char(c) if fields[focus].1.len() < 4096 => fields[focus].1.push(c),
                        KeyCode::Enter => {
                            match self.form_action(&kind, &fields) {
                                Ok(action) if self.connected => {
                                    return Some(Intent::Preview(action));
                                }
                                Ok(_) => self.notice = "Disconnected; cannot submit changes".into(),
                                Err(e) => self.notice = e,
                            };
                        }
                        _ => (),
                    };
                    self.modal = Some(Modal::Form {
                        kind,
                        fields,
                        focus,
                    });
                    return None;
                }
            }
        }
        match key.code {
            KeyCode::PageDown => self.scroll = self.scroll.saturating_add(8),
            KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(8),
            KeyCode::Char('u') if self.details => {
                if let Some(s) = self.selected_stream() {
                    self.modal = Some(Modal::Dependencies {
                        items: s.inputs.clone(),
                        focus: 0,
                    });
                }
            }
            KeyCode::Char('d') if self.details => {
                if let Some(s) = self.selected_stream() {
                    self.modal = Some(Modal::Dependencies {
                        items: s.dependents.clone(),
                        focus: 0,
                    });
                }
            }
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('/') => self.filtering = true,
            KeyCode::Char('?') => self.modal = Some(Modal::Help),
            KeyCode::Tab => self.screen = self.screen.next(),
            KeyCode::Esc => self.details = false,
            KeyCode::Down | KeyCode::Char('j') | KeyCode::Up | KeyCode::Char('k') => {
                let rows = self.rows();
                if !rows.is_empty() {
                    let current = rows
                        .iter()
                        .position(|s| Some(&s.stream_id) == self.selected.as_ref())
                        .unwrap_or(0);
                    let step = if matches!(key.code, KeyCode::Down | KeyCode::Char('j')) {
                        1
                    } else {
                        rows.len() - 1
                    };
                    self.selected = Some(rows[(current + step) % rows.len()].stream_id.clone());
                }
            }
            KeyCode::Enter => {
                self.details = true;
                if let Some(id) = &self.selected {
                    return Some(Intent::Integrity(id.clone()));
                }
            }
            KeyCode::Right if self.details => self.detail_tab = (self.detail_tab + 1) % 6,
            KeyCode::Left if self.details => self.detail_tab = (self.detail_tab + 5) % 6,
            KeyCode::Char(':') if self.connected => self.modal = Some(Modal::Actions),
            KeyCode::Char('c') if self.connected => self.modal = Some(Modal::CreateType),
            KeyCode::Char('e') if self.connected => self.form("edit"),
            KeyCode::Char('i') if self.connected && self.screen == Screen::Settings => {
                self.form("interval")
            }
            KeyCode::Char('r')
                if self.connected && matches!(self.screen, Screen::Runtime | Screen::Settings) =>
            {
                self.form("workers")
            }
            _ => (),
        }
        None
    }
    fn form(&mut self, kind: &str) {
        let fields = match kind {
            "source" => vec![
                ("New stream ID", ""),
                ("Authoritative template index (0 based)", "0"),
                ("Channel: trades/depth", "trades"),
                ("Catalog symbol filter (F2 lookup)", ""),
            ],
            "merge" => vec![
                ("Name", ""),
                ("Ordered input IDs, comma separated", ""),
                ("Ordering: arrival_order/event_time", "arrival_order"),
                ("Lateness milliseconds (event_time only)", "10"),
            ],
            "convert" => vec![
                ("Name", ""),
                ("Data input ID", ""),
                ("Reference IDs, comma separated", ""),
                ("Target asset", "USDT"),
                ("Max staleness milliseconds", "1000"),
            ],
            "interval" => vec![
                ("Observation interval milliseconds", "1000"),
                ("Save default: yes/no", "no"),
            ],
            "workers" => vec![
                ("Worker target", "4"),
                ("Save startup default: yes/no", "no"),
            ],
            "edit" => {
                if let Some(s) = self.selected_stream() {
                    if let StreamDefinition::Derived(r) = &s.definition.definition {
                        let request = serde_json::to_string(r).unwrap_or_default();
                        self.modal = Some(Modal::Form {
                            kind: "edit".into(),
                            fields: vec![
                                (
                                    "Derived request (existing validated schema)".into(),
                                    request,
                                ),
                                ("Cascade dependents: yes/no".into(), "no".into()),
                            ],
                            focus: 0,
                        });
                        return;
                    }
                }
                self.notice =
                    "Source instrument identity cannot be edited; create a new definition".into();
                return;
            }
            _ => return,
        };
        self.modal = Some(Modal::Form {
            kind: kind.into(),
            fields: fields
                .into_iter()
                .map(|(a, b)| (a.into(), b.into()))
                .collect(),
            focus: 0,
        });
    }
    pub fn form_action(
        &self,
        kind: &str,
        fields: &[(String, String)],
    ) -> std::result::Result<Action, String> {
        let value = |i: usize| fields.get(i).map(|(_, v)| v.trim()).unwrap_or("");
        let ids = |i: usize| {
            value(i)
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let duration = |i: usize| -> std::result::Result<u64, String> {
            value(i)
                .parse::<u64>()
                .map_err(|_| "Duration must be whole milliseconds".into())
                .and_then(|v| v.checked_mul(1_000_000).ok_or("Duration overflow".into()))
        };
        Ok(match kind {
            "interval" => Action::SetObservationInterval {
                milliseconds: value(0)
                    .parse()
                    .map_err(|_| "Interval must be whole milliseconds")?,
                save: yes(value(1))?,
            },
            "workers" => Action::ResizeWorkers {
                workers: value(0)
                    .parse()
                    .map_err(|_| "Worker target must be an integer")?,
                save: yes(value(1))?,
            },
            "source" => {
                let index: usize = value(1)
                    .parse()
                    .map_err(|_| "Select an authoritative metadata template")?;
                let mut request=self.capabilities.as_ref().and_then(|c|c.source_templates.get(index)).cloned().ok_or("No authoritative instrument metadata available; ask administrator to populate catalog/templates")?;
                request.channel = match value(2) {
                    "trades" => SourceChannel::Trades,
                    "depth" => SourceChannel::Depth,
                    _ => return Err("Channel must be trades or depth".into()),
                };
                Action::CreateSource {
                    stream_id: value(0).into(),
                    request,
                    desired: DesiredStreamState::default(),
                }
            }
            "merge" => {
                let ordering = match value(2) {
                    "arrival_order" => MergeOrdering::ArrivalOrder,
                    "event_time" => MergeOrdering::EventTime,
                    _ => return Err("Unsupported ordering".into()),
                };
                Action::CreateDerived {
                    request: CreateDerivedStreamRequest {
                        name: value(0).into(),
                        operation: DerivedOperation::Merge,
                        inputs: ids(1),
                        reference_inputs: vec![],
                        conversion: None,
                        ordering: Some(ordering),
                        max_lateness_ns: if ordering == MergeOrdering::EventTime {
                            Some(duration(3)?)
                        } else {
                            None
                        },
                        target_quote_asset: None,
                    },
                    desired: DesiredStreamState::default(),
                }
            }
            "convert" => Action::CreateDerived {
                request: CreateDerivedStreamRequest {
                    name: value(0).into(),
                    operation: DerivedOperation::Convert,
                    inputs: vec![value(1).into()],
                    reference_inputs: ids(2),
                    conversion: Some(ConversionSettings {
                        max_staleness_ns: duration(4)?,
                        ..Default::default()
                    }),
                    ordering: None,
                    max_lateness_ns: None,
                    target_quote_asset: Some(value(3).into()),
                },
                desired: DesiredStreamState::default(),
            },
            "edit" => {
                let s = self.selected_stream().ok_or("Select a Derived stream")?;
                Action::EditDerived {
                    stream_id: s.stream_id.clone(),
                    expected_revision: s.definition.revision.to_string(),
                    request: serde_json::from_str(value(0))
                        .map_err(|_| "Invalid Derived request JSON; use the existing schema")?,
                    cascade: yes(value(1))?,
                }
            }
            _ => return Err("Unknown form".into()),
        })
    }
    pub fn draw(&self, frame: &mut Frame) {
        let size = frame.area();
        if size.width < 45 || size.height < 12 {
            frame.render_widget(
                Paragraph::new(format!(
                    "MarketForge | {}\nTerminal too narrow; resize to 45x12\nq exits UI only",
                    if self.connected {
                        "Connected"
                    } else {
                        "STALE / disconnected"
                    }
                ))
                .block(Block::default().borders(Borders::ALL)),
                size,
            );
            return;
        }
        let areas = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(size);
        let age = self
            .updated
            .map(|t| format!("{} ms", t.elapsed().as_millis()))
            .unwrap_or_else(|| "never".into());
        let stale = !self.connected
            || self
                .updated
                .is_none_or(|t| t.elapsed() > Duration::from_secs(5));
        frame.render_widget(Paragraph::new(format!("MARKETFORGE | {} | updated {age} ago\nStreams  Runtime  Integrity  Clients  Activity  Settings [Tab]",if stale{"STALE / disconnected"}else{"Connected"})).style(Style::default().fg(if stale{Color::Yellow}else{Color::Reset})).block(Block::default().borders(Borders::ALL)),areas[0]);
        let mut text = if let Some(modal) = &self.modal {
            match modal{
            Modal::Dependencies{items,focus}=>format!("DEPENDENCIES | arrows/j/k select, Enter navigate, Esc cancel\n{}",items.iter().enumerate().map(|(i,id)|format!("{} {id}",if i==*focus{">"}else{" "})).collect::<Vec<_>>().join("\n")),
            Modal::Help=>"j/k or arrows: select | Enter: details | Left/Right: detail tabs\nTab: screen | /: filter | c: guided create | e: edit Derived\n: Actions: Enable/Disable, On demand/Always on, Run temporarily, Release my hold, Restart, Delete, Shutdown\nr on Runtime/Settings: resize workers | Esc: back | q: exits UI only\nChanges require a reviewed management plan and Enter confirmation. Temporary leases expire after abnormal exit.".into(),
            Modal::Actions=>"ACTIONS (Esc cancels)\ne Enable / Disable\na On demand / Always on\nt Run temporarily (60-second renewable hold)\nl Release my hold\nr Restart (requires impact review)\nR Restart including dependents (explicit cascade review)\nd Delete definition (requires impact review)\ns Application shutdown (administrator; requires impact review)".into(),
            Modal::CreateType=>"CREATE: 1 Source   2 Merge   3 Convert\nSource templates come from management's authoritative metadata.\nMissing metadata remains unavailable.".into(),
            Modal::Review(plan)=>format!("REVIEW {}\nAction: {:?}\nAffected: {}\nTransitions: {}\nContinuity: {}\nPersistence: {}\nWarnings: {}\nExpires: {} s\nEnter: confirm and apply | Esc: cancel",plan.id,plan.action,plan.affected_streams.join(", "),plan.transitions.join("; "),plan.continuity_impact,plan.persistence,plan.warnings.join("; "),plan.expires_in_seconds),
            Modal::Form{kind,fields,focus}=>format!("FORM {kind} | Tab/Arrows: field | Enter: validate and preview | Esc: cancel\n{}\n{}",fields.iter().enumerate().map(|(i,(k,v))|format!("{} {k}: {v}",if i==*focus{">"}else{" "})).collect::<Vec<_>>().join("\n"),if kind=="source"{self.capabilities.as_ref().map(|c|c.source_templates.iter().enumerate().map(|(i,r)|format!("{i}: {} {} {} {:?}",r.exchange.as_str(),r.symbol,r.instrument_id,r.quantity_type)).collect::<Vec<_>>().join("\n")).unwrap_or_else(||"No metadata available".into())}else{String::new()})
        }
        } else if self.details {
            self.details_text()
        } else {
            match self.screen{
            Screen::Streams=>{let rows=self.rows();let throughput=self.overview.as_ref().and_then(|v|v.metrics.as_ref()).and_then(|v|v.get("throughput"));let rate=|name:&str|throughput.and_then(|v|v.get(name)).and_then(|v|v.as_f64()).map(|v|format!("{v:.1}/s")).unwrap_or_else(||"unavailable".into());let mut text=format!("Source input: {} | Derived output: {}\n",rate("source_events_per_second"),rate("derived_events_per_second"));text.push_str(&format!("Streams | filter: {}{}\nSTREAM  TYPE  POLICY  RUNTIME  READINESS  INTEGRITY\n",self.filter,if self.filtering{" [editing]"}else{""}));for s in rows{let kind=match &s.definition.definition{StreamDefinition::Source(_)=>"Source",StreamDefinition::Derived(r)=>r.operation.as_str()};text.push_str(&format!("{} {}  {}  {:?}  {}  {}  {}\n",if Some(&s.stream_id)==self.selected.as_ref(){">"}else{" "},s.stream_id,kind,s.definition.desired.activation_policy,s.runtime,s.readiness,s.integrity));}text},
            Screen::Runtime=>self.overview.as_ref().map(|v|format!("PROCESSING POOL (r: resize)\nStartup default: {:?}\nRequested: {}  Live: {}  Starting: {}  Retiring: {}\nExecuting turns: {}  Registered processors: {}\nReady: {:?}  Retained payload: {:?} events / {:?} bytes\nResize: {} {:?}\nAvailable runtime / transport / limiter / recording observations:\n{}",v.pool.saved_startup_workers,v.pool.requested_target,v.pool.live_workers,v.pool.starting_workers,v.pool.retiring_workers,v.pool.executing_turns,v.pool.registered_processors,v.pool.ready_processors,v.pool.retained_payload_events,v.pool.retained_payload_bytes,if v.pool.resizing{"In progress"}else{"Settled"},v.pool.resize_error,v.metrics.as_ref().map(|m|m.to_string()).unwrap_or_else(||"Unavailable".into()))).unwrap_or_else(||"Awaiting runtime observations".into()),
            Screen::Integrity=>self.overview.as_ref().map(|v|v.streams.iter().filter(|s|matches!(s.definition.definition,StreamDefinition::Source(_))).map(|s|format!("{}: {}\nAffected dependents: {:?}",s.stream_id,s.integrity,s.dependents)).collect::<Vec<_>>().join("\n")).unwrap_or_else(||"Integrity unavailable".into()),
            Screen::Clients=>format!("CLIENTS / DEMAND\n{}\nThis UI's temporary leases: {:?}",self.clients,self.leases),
            Screen::Activity=>self.overview.as_ref().map(|v|v.operations.iter().map(|o|format!("{} {} | saved={} activation={} ready={:?} {:?}",o.id,o.state,o.saved,o.activation_requested,o.runtime_ready,o.detail)).collect::<Vec<_>>().join("\n")).unwrap_or_else(||"No operation observations".into()),
            Screen::Settings=>format!("SETTINGS (r: processing workers)\n{}",self.settings.iter().map(|s|format!("{}: effective={} desired={} | {} | saved={}",s.name,s.effective,s.desired,s.mutability,s.saved)).collect::<Vec<_>>().join("\n")),
        }
        };
        if self.screen == Screen::Activity && self.modal.is_none() {
            text.push_str("\nAdministrative notifications (bounded):\n");
            for e in self.activity.iter().rev() {
                text.push_str(&format!("{} {} {}\n", e.cursor, e.kind, e.detail));
            }
        }
        text = safe_text_lines(&text);
        frame.render_widget(
            Paragraph::new(text).scroll((self.scroll, 0)).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("{:?}", self.screen)),
            ),
            areas[1],
        );
        frame.render_widget(
            Paragraph::new(format!(
                "{}\nEnter Details  c Create  e Edit  / Filter  : Actions  ? Help  q Exit UI",
                safe_text(&self.notice)
            ))
            .block(Block::default().borders(Borders::ALL)),
            areas[2],
        );
    }
    fn details_text(&self) -> String {
        let Some(s) = self.selected_stream() else {
            return "Selection unavailable".into();
        };
        let tabs = [
            "Overview",
            "Dependencies",
            "Integrity",
            "Metrics",
            "Definition",
            "Activity",
        ];
        let body = match self.detail_tab {
            0 => format!(
                "WHY RUNNING? {}\nWHY NOT READY? {}\nRuntime {} / {}  Generation {}  Definition revision {}\nLast data age {:?} (None = unavailable)\nINPUTS {:?}\nREFERENCE INPUTS {:?}\nDEMAND {:?}",
                s.why_running.join(" + "),
                s.reason.as_deref().unwrap_or(&s.readiness),
                s.runtime,
                s.readiness,
                s.generation,
                s.definition.revision,
                s.last_data_age_ms,
                s.inputs,
                s.reference_inputs,
                s.direct_owners
            ),
            1 => format!(
                "Data inputs {:?}\nReference inputs {:?}\nDependents {:?}\nUse list selection to navigate upstream/downstream IDs",
                s.inputs, s.reference_inputs, s.dependents
            ),
            2 => self.integrity.to_string(),
            3 => self
                .overview
                .as_ref()
                .and_then(|v| v.metrics.as_ref())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Metrics unavailable".into()),
            4 => serde_json::to_string_pretty(&s.definition).unwrap_or_default(),
            _ => self
                .overview
                .as_ref()
                .map(|v| format!("{:?}", v.operations))
                .unwrap_or_default(),
        };
        format!(
            "{} | {} | Left/Right tabs, u upstream, d dependents, Esc list\n{body}",
            s.stream_id, tabs[self.detail_tab]
        )
    }
}
fn yes(value: &str) -> std::result::Result<bool, String> {
    match value {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err("Use yes or no".into()),
    }
}
fn safe_text_lines(text: &str) -> String {
    text.lines().map(safe_text).collect::<Vec<_>>().join("\n")
}
pub async fn run(client: MarketForgeManagementClient) -> std::result::Result<(), String> {
    use crossterm::{
        event, execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    };
    struct TerminalGuard;
    impl Drop for TerminalGuard {
        fn drop(&mut self) {
            let _ = disable_raw_mode();
            let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        }
    }
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        previous(info);
    }));
    enable_raw_mode().map_err(|e| e.to_string())?;
    let _guard = TerminalGuard;
    execute!(std::io::stdout(), EnterAlternateScreen).map_err(|e| e.to_string())?;
    let mut terminal =
        ratatui::Terminal::new(ratatui::backend::CrosstermBackend::new(std::io::stdout()))
            .map_err(|e| e.to_string())?;
    let mut backend = HttpBackend::start(client.clone());
    let mut app = App::default();
    let mut tick = tokio::time::interval(Duration::from_millis(30));
    while !app.quit {
        tokio::select! {update=backend.recv()=>if let Some(update)=update{app.update(update);},_=tick.tick()=>{for _ in 0..16{if !event::poll(Duration::ZERO).map_err(|e|e.to_string())?{break;}if let event::Event::Key(key)=event::read().map_err(|e|e.to_string())?{if let Some(intent)=app.input(key){if backend.send(intent).is_err(){app.notice="UI request capacity reached; retry deliberately".into();}}}}terminal.draw(|frame|app.draw(frame)).map_err(|e|e.to_string())?;}}
    }
    for lease in app.leases {
        let _ = tokio::time::timeout(Duration::from_secs(2), client.release_lease(&lease.id)).await;
    }
    backend.close().await;
    client.close().await;
    Ok(())
}
