/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/**/*.{js,ts,jsx,tsx}',
    '../frontend/src/pages/**/*.{js,ts,jsx,tsx}',
    '../frontend/src/components/**/*.{js,ts,jsx,tsx}',
    '../packages/ui-shared/src/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
