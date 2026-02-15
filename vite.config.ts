import { defineConfig } from "vite";

export default defineConfig({
  root: "src-frontend",
  build: {
    target: "ES2022",
    outDir: "../build",
    emptyOutDir: true,
    sourcemap: true,
  },
  publicDir: "../public",
  server: {
    port: 3000,
    strictPort: true,
  },
  clearScreen: false,
  envPrefix: ["VITE_", "TAURI_"],
});
