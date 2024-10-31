import * as path from 'path';
import { defineConfig } from 'rspress/config';

export default defineConfig({
    root: path.join(__dirname, 'docs'),
    title: 'filterx',
    description: "filterx's docs",
    icon: '/filterx-icon.png',
    logo: {
        light: '/filterx-icon.png',
        dark: '/filterx-icon.png',
    },
    themeConfig: {
        socialLinks: [
            { icon: 'github', mode: 'link', content: 'https://github.com/dwpeng/filterx' },
        ],
        enableScrollToTop: true,
    },
    markdown: {
        defaultWrapCode: true,
        showLineNumbers: true,
        mdxRs: true,
    },
});
