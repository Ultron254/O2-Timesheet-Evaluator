import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendTarget =
  process.env.VITE_API_BASE ||
  process.env.VITE_PROXY_TARGET ||
  "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 4200,
    allowedHosts: [".ngrok-free.app", ".ngrok.io"],
    proxy: {
      "/api": {
        target: backendTarget,
        changeOrigin: true,
      },
    },
  },
});
