
import {usePluginData} from '@docusaurus/useGlobalData';
import {useLocation} from '@docusaurus/router';

/**
 * This is a workaround for docusaurus not making links versioned by default.
 * Use VersionedLink instead of accessing this directly.
 * 
 * Example use: `const path = useVersionedPath('/my-doc')`
 *   outputs `/docs/my-doc` if the current version is "current"
 *   outputs `/docs/1.1.1/my-doc` if the current version is "1.1.1"
 */
export const useVersionedPath = (path) => {
    if (!path.startsWith('/')) {
        throw Error("paths to useVersionedLink must start with a slash")
    }
    const {pathname} = useLocation();
    const {versions} = usePluginData('docusaurus-plugin-content-docs');
    for (const version of Object.values(versions)) {
        if (version.path === '/docs') {
            // Skip the "current" version, since it matches all cases
            continue;
        }

        if (pathname.startsWith(`${version.path}/`)) {
            return `${version.path}${path}`
        }
    }
    // fall back to current docs, since we skipped it earlier
    return `/docs${path}`
}
