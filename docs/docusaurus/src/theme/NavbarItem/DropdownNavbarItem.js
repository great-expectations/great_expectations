import React from 'react';
import DropdownNavbarItem from '@theme-original/NavbarItem/DropdownNavbarItem';

export default function DropdownNavbarItemWrapper(props) {
  return (
    <DropdownNavbarItem {...props} href={'#'} />
  );
}
