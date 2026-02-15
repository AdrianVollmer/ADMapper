import { defineConfig } from "vite";

export default defineConfig({
  build: {
    target: "ES2022",
    outDir: "dist",
    sourcemap: true,
  },
  server: {
    port: 3000,
    open: true,
  },
});
