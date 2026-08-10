//! The terminal UI (ADR 0056 T2/T3).
//!
//! **Two panes, not three.** At 80×24 a third pane leaves about six lines of output, which
//! is useless for a bench run — so the right pane is *Detail* while browsing and *Output*
//! while running. You are either choosing or watching, never both.
//!
//! **A collapsible group tree, not a flat filtered list.** The set is expected to grow and
//! discoverability is the whole reason the tool exists.
//!
//! **One run at a time**, enforced: these scripts bind fixed ports and start containers.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand as _;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

use crate::manifest::{Manifest, Task};
use crate::preflight;
use crate::runner::{Outcome, Run};
use crate::{env, teardown};

/// A row in the left-hand tree: a group heading or a task under it.
enum Row<'a> {
    Group {
        name: &'a str,
        open: bool,
        count: usize,
    },
    Task(&'a Task),
}

/// What the right pane is doing.
#[derive(PartialEq, Eq)]
enum Mode {
    Browse,
    Output,
    Env,
    Environment,
    Confirm,
}

pub struct App<'a> {
    manifest: &'a Manifest,
    root: PathBuf,
    collapsed: BTreeMap<String, bool>,
    selected: usize,
    list_state: ListState,
    mode: Mode,
    run: Option<Run>,
    /// Per-task environment overrides, kept for the session.
    overrides: BTreeMap<String, BTreeMap<String, String>>,
    env_field: usize,
    env_buffer: String,
    scroll: u16,
    follow: bool,
    status: String,
    environment: Option<teardown::Environment>,
    quit: bool,
}

impl<'a> App<'a> {
    pub fn new(manifest: &'a Manifest, root: PathBuf) -> Self {
        Self {
            manifest,
            root,
            collapsed: BTreeMap::new(),
            selected: 0,
            list_state: ListState::default().with_selected(Some(0)),
            mode: Mode::Browse,
            run: None,
            overrides: BTreeMap::new(),
            env_field: 0,
            env_buffer: String::new(),
            scroll: 0,
            follow: true,
            status: String::new(),
            environment: None,
            quit: false,
        }
    }

    fn rows(&self) -> Vec<Row<'_>> {
        let mut rows = Vec::new();
        for (group, tasks) in self.manifest.visible_by_group() {
            let open = !self.collapsed.get(group).copied().unwrap_or(false);
            rows.push(Row::Group {
                name: group,
                open,
                count: tasks.len(),
            });
            if open {
                rows.extend(tasks.into_iter().map(Row::Task));
            }
        }
        rows
    }

    fn selected_task(&self) -> Option<&'a Task> {
        match self.rows().get(self.selected) {
            Some(Row::Task(t)) => self.manifest.get(&t.id),
            _ => None,
        }
    }

    fn overrides_for(&self, id: &str) -> BTreeMap<String, String> {
        self.overrides.get(id).cloned().unwrap_or_default()
    }

    pub fn run(mut self) -> io::Result<()> {
        enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        let mut term = Terminal::new(CrosstermBackend::new(io::stdout()))?;

        let result = self.loop_(&mut term);

        disable_raw_mode()?;
        io::stdout().execute(LeaveAlternateScreen)?;

        // Quitting must not leave the machine littered. What is guaranteed is that the
        // group was signalled and waited for; what survived is reported (ADR 0056 §4).
        if let Some(mut run) = self.run.take() {
            if run.outcome.is_none() {
                eprintln!("mqttui: stopping '{}'…", run.task_id);
                run.cancel();
                let deadline = Instant::now() + Duration::from_secs(20);
                while run.poll().is_none() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            eprint!("{}", teardown::report(&run.task_id));
        }
        result
    }

    fn loop_(&mut self, term: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
        while !self.quit {
            if let Some(run) = &mut self.run {
                if let Some(outcome) = run.poll() {
                    let verdict = match outcome {
                        Outcome::Exited(0) => "finished".to_string(),
                        Outcome::Exited(c) => format!("exited {c}"),
                        Outcome::Cancelled => "cancelled".to_string(),
                        Outcome::Failed => "could not start".to_string(),
                    };
                    if !self.status.starts_with("done:") {
                        self.status = format!(
                            "done: {} {verdict} · full log {}",
                            run.task_id,
                            run.log_path().display()
                        );
                        // A failure is more useful at its first bad line than at its tail.
                        if !matches!(outcome, Outcome::Exited(0)) {
                            if let Some(i) = run.first_bad() {
                                self.follow = false;
                                self.scroll = u16::try_from(i).unwrap_or(0);
                            }
                        }
                    }
                }
            }
            term.draw(|f| self.draw(f))?;
            if event::poll(Duration::from_millis(120))? {
                if let Event::Key(k) = event::read()? {
                    if k.kind == KeyEventKind::Press {
                        self.key(k.code, k.modifiers);
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn key(&mut self, code: KeyCode, mods: KeyModifiers) {
        match self.mode {
            Mode::Env => return self.env_key(code),
            Mode::Confirm => {
                match code {
                    KeyCode::Char('y') => {
                        self.mode = Mode::Browse;
                        self.start_selected(true);
                    }
                    _ => self.mode = Mode::Browse,
                }
                return;
            }
            Mode::Environment => {
                match code {
                    KeyCode::Enter => self.environment = Some(teardown::Environment::probe()),
                    KeyCode::Char('k') => {
                        let n = teardown::kill_stray_brokers();
                        self.status = format!("killed {n} stray broker(s)");
                        self.environment = Some(teardown::Environment::probe());
                    }
                    KeyCode::Esc | KeyCode::Char('q' | 'E') => self.mode = Mode::Browse,
                    _ => {}
                }
                return;
            }
            Mode::Browse | Mode::Output => {}
        }

        let running = self.run.as_ref().is_some_and(|r| r.outcome.is_none());
        match code {
            KeyCode::Char('q') | KeyCode::Esc if self.mode == Mode::Output => {
                self.mode = Mode::Browse;
            }
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('c') if mods.contains(KeyModifiers::CONTROL) => self.quit = true,
            KeyCode::Char('c') => {
                if let Some(run) = &mut self.run {
                    if run.outcome.is_none() {
                        run.cancel();
                        self.status = format!("cancelling {}…", run.task_id);
                    }
                }
            }
            KeyCode::Char('E') => {
                self.environment = Some(teardown::Environment::probe());
                self.mode = Mode::Environment;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.mode == Mode::Output {
                    self.follow = false;
                    self.scroll = self.scroll.saturating_add(1);
                } else {
                    self.selected = (self.selected + 1).min(self.rows().len().saturating_sub(1));
                    self.list_state.select(Some(self.selected));
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.mode == Mode::Output {
                    self.follow = false;
                    self.scroll = self.scroll.saturating_sub(1);
                } else {
                    self.selected = self.selected.saturating_sub(1);
                    self.list_state.select(Some(self.selected));
                }
            }
            KeyCode::Char('f') if self.mode == Mode::Output => self.follow = !self.follow,
            KeyCode::Char('o') => {
                if self.run.is_some() {
                    self.mode = Mode::Output;
                }
            }
            KeyCode::Char('e') => {
                if let Some(t) = self.selected_task() {
                    if !t.env.is_empty() {
                        self.env_field = 0;
                        self.env_buffer =
                            env::current(t, &self.overrides_for(&t.id), &t.env[0].name).to_string();
                        self.mode = Mode::Env;
                    }
                }
            }
            KeyCode::Left | KeyCode::Char('h') => self.toggle_group(true),
            KeyCode::Right | KeyCode::Char('l') => self.toggle_group(false),
            KeyCode::Enter => {
                if running {
                    self.status = "one run at a time — cancel the current task first".into();
                } else if let Some(t) = self.selected_task() {
                    match self.rows().get(self.selected) {
                        Some(Row::Group { .. }) => self.toggle_group_at(self.selected),
                        _ => {
                            if t.caution.is_some() {
                                self.mode = Mode::Confirm;
                            } else {
                                self.start_selected(false);
                            }
                        }
                    }
                } else {
                    self.toggle_group_at(self.selected);
                }
            }
            _ => {}
        }
    }

    fn env_key(&mut self, code: KeyCode) {
        let Some(task) = self.selected_task() else {
            self.mode = Mode::Browse;
            return;
        };
        match code {
            KeyCode::Esc => self.mode = Mode::Browse,
            KeyCode::Char(c) => self.env_buffer.push(c),
            KeyCode::Backspace => {
                self.env_buffer.pop();
            }
            KeyCode::Tab | KeyCode::Down | KeyCode::Enter => {
                let name = task.env[self.env_field].name.clone();
                self.overrides
                    .entry(task.id.clone())
                    .or_default()
                    .insert(name, self.env_buffer.clone());
                if code == KeyCode::Enter && self.env_field + 1 == task.env.len() {
                    self.mode = Mode::Browse;
                } else {
                    self.env_field = (self.env_field + 1) % task.env.len();
                    self.env_buffer = env::current(
                        task,
                        &self.overrides_for(&task.id),
                        &task.env[self.env_field].name,
                    )
                    .to_string();
                }
            }
            _ => {}
        }
    }

    fn toggle_group(&mut self, collapse: bool) {
        if let Some(Row::Group { name, .. }) = self.rows().get(self.selected) {
            self.collapsed.insert((*name).to_string(), collapse);
        }
    }

    fn toggle_group_at(&mut self, idx: usize) {
        if let Some(Row::Group { name, open, .. }) = self.rows().get(idx) {
            let name = (*name).to_string();
            let open = *open;
            self.collapsed.insert(name, open);
        }
    }

    fn start_selected(&mut self, _confirmed: bool) {
        let Some(task) = self.selected_task() else {
            return;
        };
        let missing = preflight::missing_required(task);
        if !missing.is_empty() {
            self.status = format!("cannot run: missing {}", missing.join(", "));
            return;
        }
        match Run::start(task, &self.root, &self.overrides_for(&task.id)) {
            Ok(run) => {
                self.status = format!("running {}", task.id);
                self.run = Some(run);
                self.mode = Mode::Output;
                self.follow = true;
                self.scroll = 0;
            }
            Err(e) => self.status = format!("could not start: {e}"),
        }
    }

    // ── drawing ─────────────────────────────────────────────────────────────────────
    fn draw(&mut self, f: &mut Frame) {
        let outer = Layout::vertical([Constraint::Min(3), Constraint::Length(2)]).split(f.area());
        let panes =
            Layout::horizontal([Constraint::Length(26), Constraint::Min(20)]).split(outer[0]);

        self.draw_tree(f, panes[0]);
        match self.mode {
            Mode::Output => self.draw_output(f, panes[1]),
            Mode::Environment => self.draw_environment(f, panes[1]),
            _ => self.draw_detail(f, panes[1]),
        }
        self.draw_status(f, outer[1]);

        if self.mode == Mode::Confirm {
            self.draw_confirm(f);
        }
    }

    fn draw_tree(&mut self, f: &mut Frame, area: Rect) {
        let running_id = self
            .run
            .as_ref()
            .filter(|r| r.outcome.is_none())
            .map(|r| r.task_id.clone());
        let items: Vec<ListItem> = self
            .rows()
            .iter()
            .map(|row| match row {
                Row::Group { name, open, count } => ListItem::new(Line::from(vec![Span::styled(
                    format!("{} {name} ({count})", if *open { "▾" } else { "▸" }),
                    Style::default().add_modifier(Modifier::BOLD),
                )])),
                Row::Task(t) => {
                    let blocked = !preflight::missing_required(t).is_empty();
                    let marker = if running_id.as_deref() == Some(t.id.as_str()) {
                        "●"
                    } else if blocked {
                        "!"
                    } else {
                        " "
                    };
                    let style = if blocked {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default()
                    };
                    ListItem::new(Line::from(vec![Span::styled(
                        format!("  {marker} {}", t.name),
                        style,
                    )]))
                }
            })
            .collect();
        let list = List::new(items)
            .block(Block::default().borders(Borders::RIGHT).title(" mqttui "))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        f.render_stateful_widget(list, area, &mut self.list_state);
    }

    fn draw_detail(&self, f: &mut Frame, area: Rect) {
        let Some(t) = self.selected_task() else {
            f.render_widget(
                Paragraph::new("Select a task.").block(Block::default()),
                area,
            );
            return;
        };
        let mut lines: Vec<Line> = vec![
            Line::from(Span::styled(
                t.name.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(
                format!("{}   {}", t.script, t.duration),
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
        ];
        for para in t.about.trim().lines() {
            lines.push(Line::from(para.to_string()));
        }
        if let Some(c) = &t.caution {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("! {c}"),
                Style::default().fg(Color::Yellow),
            )));
        }

        let report = preflight::check(t);
        if !report.required.is_empty() || !report.optional.is_empty() {
            lines.push(Line::from(""));
            lines.push(tool_line("Requires", &report.required, Color::Red));
            if !report.optional.is_empty() {
                lines.push(tool_line("Optional", &report.optional, Color::Yellow));
            }
        }

        if !t.env.is_empty() {
            let over = self.overrides_for(&t.id);
            lines.push(Line::from(""));
            for e in &t.env {
                let v = env::current(t, &over, &e.name);
                let shown = if v.is_empty() { "(unset)" } else { v };
                lines.push(Line::from(vec![
                    Span::styled(format!("{:<14}", e.name), Style::default().fg(Color::Cyan)),
                    Span::raw(format!("{shown:<24}")),
                    Span::styled(e.help.clone(), Style::default().fg(Color::DarkGray)),
                ]));
            }
        }

        if self.mode == Mode::Env {
            if let Some(field) = t.env.get(self.env_field) {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{} = ", field.name),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(
                        format!("{}▌", self.env_buffer),
                        Style::default().add_modifier(Modifier::REVERSED),
                    ),
                ]));
            }
        }

        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().padding(ratatui::widgets::Padding::horizontal(1))),
            area,
        );
    }

    fn draw_output(&self, f: &mut Frame, area: Rect) {
        let Some(run) = &self.run else { return };
        let lines = run.snapshot();
        let height = area.height.saturating_sub(2) as usize;
        let scroll = if self.follow {
            u16::try_from(lines.len().saturating_sub(height)).unwrap_or(0)
        } else {
            self.scroll
        };
        let body: Vec<Line> = lines
            .iter()
            .map(|l| {
                let style = if l.bad {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(l.text.clone(), style))
            })
            .collect();

        let elapsed = run.started.elapsed().as_secs();
        let head = match run.outcome {
            None if run.cancelling => format!(" {} · stopping… {elapsed}s ", run.task_id),
            None => format!(
                " {} · {elapsed}s {} ",
                run.task_id,
                if self.follow { "▼" } else { "" }
            ),
            Some(Outcome::Exited(0)) => format!(" ✓ {} · {elapsed}s ", run.task_id),
            Some(Outcome::Exited(c)) => format!(" ✗ {} · exit {c} ", run.task_id),
            Some(Outcome::Cancelled) => format!(" ⏹ {} · cancelled ", run.task_id),
            Some(Outcome::Failed) => format!(" ✗ {} · could not start ", run.task_id),
        };
        f.render_widget(
            Paragraph::new(body)
                .scroll((scroll, 0))
                .block(Block::default().title(head).borders(Borders::TOP)),
            area,
        );
    }

    fn draw_environment(&self, f: &mut Frame, area: Rect) {
        let text = self
            .environment
            .as_ref()
            .map_or_else(|| "probing…".to_string(), teardown::Environment::render);
        f.render_widget(
            Paragraph::new(text).wrap(Wrap { trim: false }).block(
                Block::default()
                    .title(" environment ")
                    .borders(Borders::TOP)
                    .padding(ratatui::widgets::Padding::horizontal(1)),
            ),
            area,
        );
    }

    fn draw_confirm(&self, f: &mut Frame) {
        let Some(t) = self.selected_task() else {
            return;
        };
        let area = centered(60, 30, f.area());
        f.render_widget(Clear, area);
        let text = vec![
            Line::from(Span::styled(
                t.name.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(
                t.caution.clone().unwrap_or_default(),
                Style::default().fg(Color::Yellow),
            )),
            Line::from(""),
            Line::from("y run    n cancel"),
        ];
        f.render_widget(
            Paragraph::new(text).wrap(Wrap { trim: false }).block(
                Block::default()
                    .title(" confirm ")
                    .borders(Borders::ALL)
                    .padding(ratatui::widgets::Padding::horizontal(1)),
            ),
            area,
        );
    }

    fn draw_status(&self, f: &mut Frame, area: Rect) {
        let keys = match self.mode {
            Mode::Output => "↑↓ scroll  f follow  c cancel  esc back  q quit",
            Mode::Env => "type to edit  tab next  ⏎ save  esc cancel",
            Mode::Environment => "⏎ refresh  k kill stray brokers  esc back",
            Mode::Confirm => "y run  n cancel",
            Mode::Browse => "↑↓ move  ←→ fold  ⏎ run  e env  o output  E environment  q quit",
        };
        let line1 = Line::from(Span::styled(
            format!(" {keys}"),
            Style::default().fg(Color::DarkGray),
        ));
        let line2 = Line::from(Span::styled(
            format!(" {}", self.status),
            Style::default().fg(Color::Cyan),
        ));
        f.render_widget(Paragraph::new(vec![line1, line2]), area);
    }
}

fn tool_line<'a>(label: &'a str, tools: &[(String, bool)], missing_colour: Color) -> Line<'a> {
    let mut spans = vec![Span::styled(
        format!("{label:<10}"),
        Style::default().fg(Color::DarkGray),
    )];
    for (name, present) in tools {
        spans.push(Span::styled(
            format!("{} {name}  ", if *present { '✓' } else { '✗' }),
            if *present {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(missing_colour)
            },
        ));
    }
    Line::from(spans)
}

fn centered(pct_x: u16, pct_y: u16, area: Rect) -> Rect {
    let v = Layout::vertical([
        Constraint::Percentage((100 - pct_y) / 2),
        Constraint::Percentage(pct_y),
        Constraint::Percentage((100 - pct_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - pct_x) / 2),
        Constraint::Percentage(pct_x),
        Constraint::Percentage((100 - pct_x) / 2),
    ])
    .split(v[1])[1]
}
