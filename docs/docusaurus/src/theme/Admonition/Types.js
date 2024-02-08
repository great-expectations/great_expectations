import React from 'react';
import DefaultAdmonitionTypes from '@theme-original/Admonition/Types';

function CtaAdmonition(props) {
    return (
        <div>
            <h3>{props.title}</h3>
            <div>{props.children}</div>
        </div>
    );
}

const AdmonitionTypes = {
    ...DefaultAdmonitionTypes,

    // Custom admonitions
    'cta': CtaAdmonition,
};

export default AdmonitionTypes;
