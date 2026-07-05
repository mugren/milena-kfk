# Dockview Workspace Prototype Notes

Question: can Dockview render Milena's planned tab/group workspace while Milena
keeps canonical tab state?

Current answer from the spike:

- Dockview provides the core mechanics Milena needs: tabbed groups, resizable
  split groups, drag/drop, programmatic panel moves, and group maximize/restore.
- User review: the general look and feel is good enough to proceed toward
  production planning.
- User review: the border treatment between tabs needs improvement before the
  production implementation.
- User review: per-tab publisher drafts are not clearly necessary. The prototype
  used mock per-tab drafts only to demonstrate that tab-owned state survives
  movement; production should not treat per-tab drafts as settled behavior.
- The prototype keeps tab-owned mock Kafka state in React and uses Dockview as
  the renderer/interaction adapter.
- Explicit Open, Move right, Move bottom, close, maximize, and restore are
  routed through Milena-owned handlers before calling Dockview.
- Drag/drop constraints are partly library-led. `onWillDrop` can block left/top
  and cap violations before the drop, but Dockview still owns some intermediate
  drag state and the prototype resyncs canonical group state from `api.groups`
  after drop/move events.
- Adjacent-group targeting for Move right/bottom is inferred from rendered group
  geometry in the prototype. A production migration should either keep that
  adapter small and tested or map Dockview serialization into a pure layout tree.
- Floating groups are disabled with `disableFloatingGroups`; tab/group context
  menus are empty to avoid close-all/close-others/popout affordances.
- Popout APIs remain available on the Dockview API object. The prototype does
  not expose them, but production code should avoid passing any popout controls
  through custom menus or actions.
- Dockview theming is selected through the `theme` option and matching CSS
  classes from `dockview-react/dist/styles/dockview.css`. This prototype uses
  `themeLight` and then scopes Milena token overrides under the prototype
  workspace.
- In local Vite testing, the Dockview prototype is intentionally rendered
  outside `React.StrictMode`. With StrictMode around the Dockview surface, a
  single Open action produced an extra Dockview panel without matching canonical
  React tab state. Production migration should verify Dockview's StrictMode
  behavior separately before embedding it under the production root wrapper.
- The adapter prunes any Dockview panel that lacks a matching Milena tab record.
  This is a prototype guardrail for the architectural decision that Milena owns
  product state. If Dockview ever emits an unexpected panel id, the visual layout
  is repaired back to canonical state instead of accepting the library as truth.
- The prototype schedules a few delayed sync passes after `addPanel` because
  Dockview can finish some panel mutations after the initial React click handler
  and immediate event callbacks. Production code should replace this with a
  tighter adapter contract or a pure layout reducer around Dockview events.

Run with:

```sh
npm run prototype:dockview
```

The normal app can also show the prototype with `?prototype=dockview`.
