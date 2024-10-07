// Temporary workaround to creating custom navbar items according to the documentation when
// running 'yarn swixxle --list'
// See https://github.com/facebook/docusaurus/issues/7227.
import ComponentTypes from '@theme-original/NavbarItem/ComponentTypes';
import GithubNavbarItem from '@site/src/components/NavbarItems/GithubNavbarItem';
import ColorModeToggle from '@site/src/components/NavbarItems/ColorModeToggle';

export default {
    ...ComponentTypes,
    'custom-githubNavbarItem': GithubNavbarItem,
    'custom-colorModeToggle': ColorModeToggle,
};
