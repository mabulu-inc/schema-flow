import { defineConfig } from "vitepress";

export default defineConfig({
  title: "schema-flow",
  description: "Declarative zero-downtime PostgreSQL migrations",
  base: "/schema-flow/",
  srcExclude: ["README.md", "CLAUDE.md.template"],
  head: [["link", { rel: "icon", type: "image/svg+xml", href: "/schema-flow/logo.svg" }]],
  themeConfig: {
    nav: [
      { text: "Guide", link: "/getting-started" },
      { text: "YAML Reference", link: "/yaml-reference" },
      { text: "Examples", link: "/examples" },
      {
        text: "GitHub",
        link: "https://github.com/mabulu-inc/schema-flow",
      },
    ],
    sidebar: [
      {
        text: "Introduction",
        items: [
          { text: "What is schema-flow?", link: "/" },
          { text: "Getting Started", link: "/getting-started" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "YAML Reference", link: "/yaml-reference" },
          { text: "CLI Commands", link: "/cli" },
        ],
      },
      {
        text: "Switching to schema-flow",
        items: [
          { text: "From Imperative Tools", link: "/switching-from-imperative" },
          { text: "From ORM Migrations", link: "/switching-from-orms" },
        ],
      },
      {
        text: "Examples",
        items: [{ text: "Example Projects", link: "/examples" }],
      },
      {
        text: "AI Integration",
        items: [{ text: "CLAUDE.md Template", link: "/ai-integration" }],
      },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/mabulu-inc/schema-flow" }],
    search: { provider: "local" },
    footer: {
      message: "Released under the MIT License.",
      copyright: "Copyright 2026 Mabulu, Inc.",
    },
  },
});
