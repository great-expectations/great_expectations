import React from 'react';
import Admonition from '@theme-original/Admonition';
import InfoIcon from "../../../static/img/admonition-info-icon.svg"
import CautionIcon from "../../../static/img/admonition-caution-icon.svg"
import DangerIcon from "../../../static/img/admonition-danger-icon.svg"
import CtaIcon from "../../../static/img/admonition-cta-icon.svg"

export default function AdmonitionWrapper(props) {

  if (props.type === 'info' || props.type === 'note' || props.type === 'tip') {
    return <Admonition {...props} icon={<InfoIcon/>} />;
  }

  if (props.type === 'caution' || props.type === 'warning') {
    return <Admonition {...props} icon={<CautionIcon/>} />;
  }

  if (props.type === 'danger') {
    return <Admonition {...props} icon={<DangerIcon/>} />;
  }

  if (props.type === 'cta') {
    return <Admonition {...props} icon={<CtaIcon/>} />;
  }
  return (
    <Admonition {...props} />
  );
}
