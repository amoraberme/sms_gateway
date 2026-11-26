/** @type {import('next').NextConfig} */
const nextConfig = {
  compress: false,
  async rewrites() {
    const backendUrl = process.env.BACKEND_INTERNAL_URL || "http://127.0.0.1:8000";
    return {
      fallback: [
        {
          source: "/api/gateway/:path*",
          destination: `${backendUrl}/:path*`,
        },
      ],
    };
  },
};

export default nextConfig;
