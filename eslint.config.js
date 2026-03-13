import eslint from "@eslint/js";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";
import prettier from "eslint-config-prettier";

export default [
  eslint.configs.recommended,
  {
    files: ["src/**/*.ts"],
    languageOptions: {
      parser: tsparser,
      parserOptions: {
        ecmaVersion: 2022,
        sourceType: "module",
      },
      globals: {
        // Browser globals
        document: "readonly",
        window: "readonly",
        console: "readonly",
        HTMLElement: "readonly",
        HTMLInputElement: "readonly",
        EventSource: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        fetch: "readonly",
        FormData: "readonly",
        Event: "readonly",
        MouseEvent: "readonly",
        KeyboardEvent: "readonly",
        Response: "readonly",
        URL: "readonly",
        Blob: "readonly",
        File: "readonly",
        FileList: "readonly",
        Promise: "readonly",
        Map: "readonly",
        Set: "readonly",
        Infinity: "readonly",
        Node: "readonly",
        CanvasRenderingContext2D: "readonly",
        performance: "readonly",
        navigator: "readonly",
        localStorage: "readonly",
        alert: "readonly",
        confirm: "readonly",
        HTMLTextAreaElement: "readonly",
        HTMLButtonElement: "readonly",
        HTMLSelectElement: "readonly",
        HTMLFormElement: "readonly",
        HTMLSpanElement: "readonly",
        CustomEvent: "readonly",
        requestAnimationFrame: "readonly",
        RequestInit: "readonly",
        AbortSignal: "readonly",
        AbortController: "readonly",
        URLSearchParams: "readonly",
        // Web Worker globals
        Worker: "readonly",
        MessageEvent: "readonly",
        DedicatedWorkerGlobalScope: "readonly",
        self: "readonly",
      },
    },
    plugins: {
      "@typescript-eslint": tseslint,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "no-unused-vars": "off",
    },
  },
  {
    files: ["src/**/__tests__/**/*.ts"],
    languageOptions: {
      globals: {
        process: "readonly",
      },
    },
  },
  prettier,
];
