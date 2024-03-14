import React from 'react';
import NavbarItem from '@theme-original/NavbarItem';
import {useNavbarMobileSidebar} from "@docusaurus/theme-common/internal";

export default function NavbarItemWrapper(props) {
    const mobileSidebar = useNavbarMobileSidebar();
   const handleMobileClick = () => {
       if(props.label === 'Home'){
           mobileSidebar.toggle()
       }
   }

  return (
      <NavbarItem {...props} onClick={props.mobile ? handleMobileClick : null} />
  );
}
