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
      return <Admonition {...props} icon={<CtaIcon/>} />;
    default:
      return <Admonition {...props} />
  }
}
