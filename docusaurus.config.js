/** @type {import('@docusaurus/types').DocusaurusConfig} */

const remarkCodeImport = require('remark-code-import')

module.exports = {
  title: 'Great Expectations',
  tagline: 'Always know what to expect from your data.',
  // TODO update for proper hosting URL once decisions are made
  url: 'https://knoxpod.netlify.com', // Url to your site with no trailing slash
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'great-expectations',
  projectName: 'great_expectations',
  themeConfig: {
    prism: {
      theme: require('prism-react-renderer/themes/vsDark'),
      customCss: require.resolve('./src/css/custom.css')
    },
    announcementBar: {
      id: 'RTD_docs', // Link to RTD Docs
      content:
        '🔄 Older Documentation for Great Expectations can be found at the Read the Docs site🔄',
      backgroundColor: '#32a852', // Defaults to `#fff`.
      textColor: '#091E42', // Defaults to `#000`.
      isCloseable: false, // Defaults to `true`.
    },
    navbar: {
      title: 'Great Expectations',
      logo: {
        alt: 'Great Expectations',
        src: 'img/logo.svg'
      },
      items: [
        {
        // TODO change this to live side by side
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'Docs',
          position: 'left'
        },
        {
          to: 'docs/0.9.9/',
          activeBasePath: 'docs',
          label: 'Old Docusaurus Docs',
          position: 'left'
        },
        {
          href: 'https://github.com/great-expectations/great_expectations',
          label: 'GitHub',
          position: 'right'
        }
      ]
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: 'docs/'
            }
          ]
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Slack',
              href: 'https://greatexpectations.io/slack'
            },
            {
              label: 'Discuss',
              href: 'https://discuss.greatexpectations.io/'
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/expectgreatdata'
            },
            {
              label: 'YouTube',
              href: 'https://www.youtube.com/c/GreatExpectationsData'
            }
          ]
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Superconductive.`
    }
  },

  // themes:[ ],
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          remarkPlugins: [remarkCodeImport],
          editUrl:
            'https://github.com/great-expectations/great_expectations/tree/develop/docs/'
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css')
        },
        lastVersion: 'current',
        versions: {

          //Example configuration:
          current: {
            label: '1.0.0-docs',
            path: '1.0.0',
          },
          '0.9.9': {
            label: '0.9.9-docs',
            path: '0.9.9',
          },
        },
      }
    ]
  ],

  // plugins: [
  //   [
  //     '@docusaurus/plugin-content-docs',
  //     {
  //       // /**
  //       //  * Path to data on filesystem relative to site dir.
  //       //  */
  //       path: 'docs',
  //       // /**
  //       //  * Base url to edit your site.
  //       //  * Docusaurus will compute the final editUrl with "editUrl + relativeDocPath"
  //       //  */
  //       editUrl: 'https://github.com/great-expectations/great_expectations/tree/develop/docs/',
  //       // /**
  //       //  * For advanced cases, compute the edit url for each markdown file yourself.
  //       //  */
  //       // editUrl: function ({
  //       //   locale,
  //       //   version,
  //       //   versionDocsDirPath,
  //       //   docPath,
  //       //   permalink,
  //       // }) {
  //       //   return `https://github.com/facebook/docusaurus/edit/master/website/${versionDocsDirPath}/${docPath}`;
  //       // },
  //       /**
  //        * Useful if you commit localized files to git.
  //        * When markdown files are localized, the edit url will target the localized file,
  //        * instead of the original unlocalized file.
  //        * Note: this option is ignored when editUrl is a function
  //        */
  //       editLocalizedFiles: false,
  //       /**
  //        * Useful if you don't want users to submit doc pull-requests to older versions.
  //        * When docs are versioned, the edit url will link to the doc
  //        * in current version, instead of the versioned doc.
  //        * Note: this option is ignored when editUrl is a function
  //        */
  //       editCurrentVersion: false,
  //       /**
  //        * URL route for the docs section of your site.
  //        * *DO NOT* include a trailing slash.
  //        * INFO: It is possible to set just `/` for shipping docs without base path.
  //        */
  //       routeBasePath: 'docs',
  //       include: ['**/*.md', '**/*.mdx'], // Extensions to include.
  //       /**
  //        * Path to sidebar configuration for showing a list of markdown pages.
  //        */
  //       sidebarPath: require.resolve('./sidebars.js'),
  //       /**
  //        * Function used to replace the sidebar items of type "autogenerated"
  //        * by real sidebar items (docs, categories, links...)
  //        */
  //       // sidebarItemsGenerator: async function ({
  //       //   defaultSidebarItemsGenerator, // useful to re-use/enhance default sidebar generation logic from Docusaurus
  //       //   numberPrefixParser, // numberPrefixParser configured for this plugin
  //       //   item, // the sidebar item with type "autogenerated"
  //       //   version, // the current version
  //       //   docs, // all the docs of that version (unfiltered)
  //       // }) {
  //       //   // Use the provided data to generate a custom sidebar slice
  //       //   return [
  //       //     {type: 'doc', id: 'intro'},
  //       //     {
  //       //       type: 'category',
  //       //       label: 'Tutorials',
  //       //       items: [
  //       //         {type: 'doc', id: 'tutorial1'},
  //       //         {type: 'doc', id: 'tutorial2'},
  //       //       ],
  //       //     },
  //       //   ];
  //       // },
  //       /**
  //        * The Docs plugin supports number prefixes like "01-My Folder/02.My Doc.md".
  //        * Number prefixes are extracted and used as position to order autogenerated sidebar items.
  //        * For conveniency, number prefixes are automatically removed from the default doc id, name, title.
  //        * This parsing logic is configurable to allow all possible usecases and filename patterns.
  //        * Use "false" to disable this behavior and leave the docs untouched.
  //        */
  //       // numberPrefixParser: function (filename) {
  //       //   // Implement your own logic to extract a potential number prefix
  //       //   const numberPrefix = findNumberPrefix(filename);
  //       //   // Prefix found: return it with the cleaned filename
  //       //   if (numberPrefix) {
  //       //     return {
  //       //       numberPrefix,
  //       //       filename: filename.replace(prefix, ''),
  //       //     };
  //       //   }
  //       //   // No number prefix found
  //       //   return {numberPrefix: undefined, filename};
  //       // },
  //       /**
  //        * Theme components used by the docs pages
  //        */
  //       //docLayoutComponent: '@theme/DocPage',
  //       //docItemComponent: '@theme/DocItem',
  //       /**
  //        * Remark and Rehype plugins passed to MDX
  //        */
  //       remarkPlugins: [remarkCodeImport],
  //       rehypePlugins: [],
  //       /**
  //        * Custom Remark and Rehype plugins passed to MDX before
  //        * the default Docusaurus Remark and Rehype plugins.
  //        */
  //       beforeDefaultRemarkPlugins: [],
  //       beforeDefaultRehypePlugins: [],
  //       /**
  //        * Whether to display the author who last updated the doc.
  //        */
  //       showLastUpdateAuthor: false,
  //       /**
  //        * Whether to display the last date the doc was updated.
  //        */
  //       showLastUpdateTime: false,
  //       /**
  //        * By default, versioning is enabled on versioned sites.
  //        * This is a way to explicitly disable the versioning feature.
  //        */
  //       disableVersioning: false,
  //       /**
  //        * Skip the next release docs when versioning is enabled.
  //        * This will not generate HTML files in the production build for documents
  //        * in `/docs/next` directory, only versioned docs.
  //        */
  //       includeCurrentVersion: true,
  //       /**
  //        * The last version is the one we navigate to in priority on versioned sites
  //        * It is the one displayed by default in docs navbar items
  //        * By default, the last version is the first one to appear in versions.json
  //        * By default, the last version is at the "root" (docs have path=/docs/myDoc)
  //        * Note: it is possible to configure the path and label of the last version
  //        * Tip: using lastVersion: 'current' make sense in many cases
  //        */
  //       lastVersion: undefined,
  //       /**
  //        * The docusaurus versioning defaults don't make sense for all projects
  //        * This gives the ability customize the label and path of each version
  //        * You may not like that default version
  //        */
  //       versions: {
  //
  //         //Example configuration:
  //         current: {
  //           label: '1.0.0-docs',
  //           path: 'version-1.0.0',
  //         },
  //         '0.9.9': {
  //           label: '0.9.9-docs',
  //           path: 'version-0.9.9',
  //         },
  //
  //       },
  //       /**
  //        * Sometimes you only want to include a subset of all available versions.
  //        * Tip: limit to 2 or 3 versions to improve startup and build time in dev and deploy previews
  //        */
  //       onlyIncludeVersions: undefined, // ex: ["current", "1.0.0", "2.0.0"]
  //     },
  //   ],
  // ]

}
