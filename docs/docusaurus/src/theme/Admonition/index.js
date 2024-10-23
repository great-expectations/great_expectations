import React from 'react';
import Admonition from '@theme-original/Admonition';
import InfoIcon from "../../../static/img/admonition-info-icon.svg"
import CautionIcon from "../../../static/img/admonition-caution-icon.svg"
import DangerIcon from "../../../static/img/admonition-danger-icon.svg"
import CtaIcon from "../../../static/img/admonition-cta-icon.svg"

export default function AdmonitionWrapper(props) {

  switch(props.type) {
    case 'info':
    case 'note':
    case 'tip':
      return <Admonition {...props} icon={<InfoIcon/>} />;
    case 'caution':
    case 'warning':
      return <Admonition {...props} icon={<CautionIcon/>} />;
    case 'danger':
      return <Admonition {...props} icon={<DangerIcon/>} />;
    case 'cta':
      if (props.icon == null) {
        // Display default CtaIcon if no other icon is supplied.
        return <Admonition {...props} icon={<CtaIcon/>} />;
      } else {
        // Enable calling admonition to specify a custom icon.
        return <Admonition {...props} icon={props.icon} />;
      }
    default:
      return <Admonition {...props} />
  }
}
