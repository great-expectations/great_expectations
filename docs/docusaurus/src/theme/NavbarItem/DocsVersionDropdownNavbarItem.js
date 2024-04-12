import React from 'react';
import DocsVersionDropdownNavbarItem from '@theme-original/NavbarItem/DocsVersionDropdownNavbarItem';
import {useLocation} from '@docusaurus/router';

export default function DocsVersionDropdownNavbarItemWrapper(props) {
    const location = useLocation();
    if(props && props.mobile && (location.pathname.includes('docs/home') || location.pathname.includes('docs/cloud')) ) return null;
    return (
        <DocsVersionDropdownNavbarItem {...props} />
    );
}
