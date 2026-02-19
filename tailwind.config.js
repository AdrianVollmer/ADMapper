/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./src/frontend/**/*.{html,js,ts,jsx,tsx}",
  ],
  // Preserve classes used dynamically in JS (e.g., toast-${type})
  safelist: [
    "toast-success",
    "toast-error",
    "toast-info",
  ],
  theme: {
    extend: {
      colors: {
        gray: {
          850: "#1a1d24",
        },
      },
    },
  },
  plugins: [],
};
