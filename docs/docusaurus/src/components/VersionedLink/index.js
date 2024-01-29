import React from 'react';
import Link from '@docusaurus/Link';
import { useVersionedPath } from '@site/src/hooks/use_versioned_path';

/**
 * Version-safe link component.
 * 
 * The `to` prop MUST be prefixed with a slash and starting from the content root.
 * e.g. <VersionedLink to="/intro" /> used in a page at `/docs/1.0.0/foo/bar` links to `docs/1.0.0/intro
 * e.g. <VersionedLink to="/intro" /> used in a page at `/docs/foo/bar` links to `docs/intro
 */
export default VersionedLink = ({to, ...rest}) => {
    const versionedPath = useVersionedPath(to);
    return (
        <Link to={versionedPath} {...rest} />
    )
}
