/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./src-frontend/**/*.{html,js,ts,jsx,tsx}",
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
