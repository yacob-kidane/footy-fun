/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    appDir: true,
  },
  images: {
    domains: ['images.unsplash.com', 'ik.imagekit.io', 'tailark.com', 'html.tailus.io'],
  },
}

module.exports = nextConfig