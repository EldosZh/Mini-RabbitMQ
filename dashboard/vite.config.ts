import { defineConfig, loadEnv } from "vite";
import tailwindcss from "@tailwindcss/vite";
export default defineConfig(({ mode }) => {
  const target =
    loadEnv(mode, ".", "MINIRABBIT_").MINIRABBIT_PROXY_TARGET ||
    "http://127.0.0.1:8080";
  return {
    plugins: [tailwindcss()],
    server: {
      proxy: {
        "/api": target,
        "/health": target,
      },
    },
  };
});
