/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  transpilePackages: ["react-plotly.js", "plotly.js", "react-grid-layout", "react-resizable"],
}

module.exports = nextConfig
