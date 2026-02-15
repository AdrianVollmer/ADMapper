#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const srcDir = "src";
const destDir = "public";

// Files and directories to copy from src-frontend/ to static/
const entries = ["favicon.svg", "manifest.json", "service-worker.js", "vendor"];

fs.mkdirSync(destDir, { recursive: true });

for (const entry of entries) {
  const src = path.join(srcDir, entry);
  const dest = path.join(destDir, entry);

  if (!fs.existsSync(src)) {
    console.warn(`Warning: ${src} not found, skipping`);
    continue;
  }

  fs.cpSync(src, dest, { recursive: true });
  console.log(`Copied: ${src} -> ${dest}`);
}

console.log("Static assets copied.");
