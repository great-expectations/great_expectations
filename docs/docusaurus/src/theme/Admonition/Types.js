import React from 'react';
import DefaultAdmonitionTypes from '@theme-original/Admonition/Types';

function CtaAdmonition(props) {
    return (
        <div className="alert--cta">
            <h5>{props.title}</h5>
            <div>{props.children}</div>
        </div>
    );
}

const AdmonitionTypes = {
    ...DefaultAdmonitionTypes,

    // Add all your custom admonition types here...
    // You can also override the default ones if you want
    'cta': CtaAdmonition,
};

export default AdmonitionTypes;