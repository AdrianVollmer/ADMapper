import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Use happy-dom for DOM simulation
    environment: "happy-dom",

    // Test file patterns
    include: ["src/frontend/__tests__/**/*.test.ts"],

    // Coverage configuration
    coverage: {
      provider: "v8",
      include: ["src/frontend/**/*.ts"],
      exclude: [
        "src/frontend/__tests__/**",
        "src/frontend/main.ts", // Entry point
      ],
      reporter: ["text", "html"],
    },

    // Global test timeout
    testTimeout: 10000,

    // Setup files (run before each test file)
    setupFiles: ["src/frontend/__tests__/setup.ts"],
  },
});
