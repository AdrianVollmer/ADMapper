import { defineConfig } from "vite";

export default defineConfig({
  root: "src-frontend",
  build: {
    target: "ES2022",
    outDir: "../build",
    emptyOutDir: true,
    sourcemap: true,
    rollupOptions: {
      output: {
        hashCharacters: "hex",
        entryFileNames: "assets/[name]-[hash:8].js",
        chunkFileNames: "assets/[name]-[hash:8].js",
        assetFileNames: "assets/[name]-[hash:8][extname]",
      },
    },
  },
  publicDir: "../public",
  server: {
    port: 3000,
    strictPort: true,
  },
  clearScreen: false,
  envPrefix: ["VITE_", "TAURI_"],
});
