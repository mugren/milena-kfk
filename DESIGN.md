# Milena Design Notes

This document captures the final prototype direction from `prototype/milena-ui`.

## Design Direction

Milena uses a modernized Kafkasque developer-workbench style: compact, neutral, low-noise, and built for repeated Kafka debugging sessions. The UI should feel closer to a focused desktop tool than a marketing dashboard.

Core principles:

- Dense but readable information.
- Soft panel boundaries, not heavy cards.
- Stream/list presentation for messages instead of boxed rows.
- Explicit Kafka activity: empty panes must look inactive until the user starts poll or publish.
- Restrained Kafka-ish accents through teal/blue status and stream markers, not decorative theming.

## Typography

Primary font stack:

```css
Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif
```

Monospace font stack for JSON, payloads, offsets, and code-like content:

```css
"SFMono-Regular", Consolas, monospace
```

Type scale:

- `h1`: 18px, line-height 1.2
- `h2`: 14px, line-height 1.25
- `h3`: 13px, line-height 1.25
- Body/helper copy: 11px, line-height 1.45
- Message payload preview: 11px monospace, line-height 1.35
- Form JSON editor: 12px monospace, line-height 1.5
- Eyebrow/status text: 10-11px, uppercase, 800 weight

## Colors

Use these CSS tokens as the base palette:

```css
--ink: #151a22;
--muted: #5f6878;
--line: #e1e5ec;
--panel: #fcfdff;
--panel-soft: #f5f7fa;
--input: #ffffff;
--app: #e9edf2;

--blue: #3457d5;
--teal: #0f766e;
--green: #18805a;
--red: #b8323b;
--amber: #a96512;

--active-bg: #eef2ff;
--pinned-bg: #f3f8f7;
--row-bg: #ffffff;
--warning-bg: #fff9ed;
--error-bg: #fff5f6;
--ack-bg: #f0faf5;

--switcher-bg: #171b22;
--switcher-panel: #2a313c;
--shadow: 0 12px 32px rgba(24, 31, 43, 0.08);
```

Usage rules:

- Use `--app` for the application canvas.
- Use `--panel` for rails, panes, and tools.
- Use `--panel-soft` for low-emphasis badges, stat cells, hover states, and activity rows.
- Use `--line` for borders and row separators.
- Use `--blue` for primary actions and selected/active pane accents.
- Use `--teal` for Kafka/topic affinity, pinned topic markers, and message topic names.
- Use `--green`, `--red`, and `--amber` only for status states.

## Layout

Desktop workspace:

```css
grid-template-columns: 240px minmax(480px, 1fr) 280px;
gap: 8px;
padding: 8px;
```

Regions:

- Left rail: environment summary and topic list.
- Center: split workspace with one, two, or four panes.
- Right rail: selected pane state, split rules, and global activity log.
- Bottom fixed prototype control is throwaway and should not ship.

Pane layouts:

- 1 pane: one focused publish-and-poll pane.
- 2 panes: two equal columns.
- 4 panes: two-by-two grid.
- Newly split panes start empty and show no Kafka activity.

Responsive behavior:

- Under 1180px, rails and workspace stack into one column.
- Under 820px, split grids collapse to one column.
- Avoid horizontal scrolling at all viewport widths.

## Density And Spacing

Spacing is compact:

- Page and split-grid gap: 8px.
- Rail/pane header padding: 8px 10px.
- Topic row min-height: 28px.
- Message row padding: 6px 8px.
- Button min-size: 28px.
- Status pill min-height: 20px.
- Pane stat cell min-height: 24px.

Prefer separators and subtle background shifts over nested cards. Avoid large empty padding inside repeated elements.

## Components

### Topics

Topic rows are compact list rows, not cards.

- Grid: `10px minmax(0, 1fr) auto`.
- Name: 11px, strong weight, ellipsized.
- Meta: 10px muted text.
- Pinned topics use a subtle 1px teal inset marker, not a heavy filled block.

### Panes

Panes are soft bordered panels with minimal shadow.

- Border: 1px `--line`.
- Background: `--panel`.
- Publish pane has a soft 2px blue inset top accent.
- Error pane uses a low-contrast red border and inline error strip.

Pane header content should show:

- Pane id and mode.
- Topic name.
- Consumer group when present.
- Compact actions for split right, split top, and stop.

### Messages

Messages are stream rows.

- Do not render each message as a boxed card.
- Use top separators between rows.
- Keep receive time, partition, offset, and key as compact muted metadata.
- Use teal for the topic label.
- Payload previews use monospace and wrap safely.
- Invalid JSON rows use `--warning-bg` and a subtle amber border.

### Publisher

The publisher sits beside consumer feedback inside the combined publish-and-poll pane.

- JSON editor uses monospace.
- Format is a secondary compact button.
- Send is the primary filled blue button.
- Producer ack is separate from consumer feedback and uses `--ack-bg`.

### Buttons

Default buttons should feel like toolbar controls:

- Transparent background by default.
- 28px minimum height.
- 12px text.
- Border appears on hover.
- Active state uses `--active-bg` and blue text.

Primary actions:

- Filled blue.
- Keep width compact, around 72px minimum.

### Status

Status pills are compact and uppercase:

- 20px minimum height.
- 10px text.
- Use muted low-contrast backgrounds.

## Visual Texture

The workspace uses a faint grid/stream texture:

```css
linear-gradient(90deg, rgba(21, 26, 34, 0.022) 1px, transparent 1px),
linear-gradient(rgba(21, 26, 34, 0.02) 1px, transparent 1px);
background-size: 28px 28px;
```

This should remain barely visible. It is a spatial cue, not decoration.

## Interaction Notes

- Pane count is a first-class workspace state: 1, 2, or 4 panes.
- Splitting a pane creates an empty pane.
- Empty panes must not imply an active consumer.
- Active sessions reset after restart.
- Producer ack/status must stay visually separate from consumer feedback.
- Pane-level errors should stay local to the pane.
- Global activity/errors live in the right rail and should be clearable.

## Things To Avoid

- Heavy cards around every message.
- Large rounded buttons or decorative pills.
- Beige/parchment novelty theming.
- High-contrast accent blocks for ordinary panels.
- Duplicate workspace tabs when pane cards already identify sessions.
- Auto-starting consumers in new split panes.
