#!/usr/bin/env node
//
// Post-process Tailwind CSS output: add a content hash to the filename
// and register it in the shared asset manifest so templates can
// reference the hashed URL for cache-busting.
//
// Usage: node scripts/hash-css.js
//   (run after `npx tailwindcss ...` writes static/css/tailwind.css)
//

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const cssDir = "build/css";
const cssFile = "tailwind.css";
const manifestPath = "build/manifest.json";

const cssPath = path.join(cssDir, cssFile);
if (!fs.existsSync(cssPath)) {
  console.error(`CSS file not found: ${cssPath}`);
  process.exit(1);
}

const content = fs.readFileSync(cssPath);
const hash = crypto
  .createHash("md5")
  .update(content)
  .digest("hex")
  .slice(0, 8);
const hashedName = `tailwind.${hash}.css`;

// Clean old hashed CSS files
for (const file of fs.readdirSync(cssDir)) {
  if (file.match(/^tailwind\.[a-f0-9]{8}\.css$/)) {
    fs.unlinkSync(path.join(cssDir, file));
  }
}

// Write hashed copy (keep the plain file for watch/dev mode)
fs.copyFileSync(cssPath, path.join(cssDir, hashedName));

// Merge into shared manifest
let manifest = {};
if (fs.existsSync(manifestPath)) {
  try {
    manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  } catch {
    // Ignore corrupt manifest; it will be regenerated
  }
}
manifest[cssFile] = hashedName;
fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

console.log(`CSS: ${cssFile} -> ${hashedName}`);
