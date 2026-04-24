Add an HTML export format to "File → Export" that produces a fully interactive,
self-contained HTML file.

The exported file:
- Loads Cytoscape.js from CDN for the graph renderer
- Preserves node positions from the current ADMapper layout
- Supports pan, zoom (scroll wheel), and node selection
- Shows a slide-in detail panel with node type and properties when a node is clicked
- Highlights the selected node's neighbourhood and dims the rest
- Has a "Fit Graph" and "Toggle Labels" toolbar button
- Matches ADMapper's dark colour scheme and node-type colour palette

Implementation:
- `src/frontend/export-graph-template.html` — standalone HTML template embedded in
  the JS bundle at build time via Vite's `?raw` import
- `exportHTML()` in `export.ts` collects graph data (id, label, type, x/y,
  properties) from the Sigma renderer and splices it into the template as an
  inline JSON blob before download
- New `EXPORT_HTML` / `"export-html"` action wired through `actions.ts`
- "Interactive HTML" menu item added to File → Export in `index.html`
