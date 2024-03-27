import React from 'react';
import DropdownNavbarItem from '@theme-original/NavbarItem/DropdownNavbarItem';

export default function DropdownNavbarItemWrapper(props) {
  return (
      // The href is overwritten to prevent default redirect when clicking on the label of the dropdown
      // With the new href the user will stay in the current page even if they click it
      <DropdownNavbarItem {...props} href={'#'} />
  );
}
