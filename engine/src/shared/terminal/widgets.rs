//! Small dashboard primitives; widgets always use the rectangle supplied by the caller.
use super::palette::MUTED;
use ratatui::{
    style::Style,
    widgets::{Block, Borders},
};
pub fn panel(title: &str) -> Block<'_> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(MUTED))
}
