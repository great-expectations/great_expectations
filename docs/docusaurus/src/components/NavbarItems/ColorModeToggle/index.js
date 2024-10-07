import React from 'react';
import ColorModeToggle from '@theme-original/Navbar/ColorModeToggle';
import styles from './styles.module.scss';

export default function CustomColorModeToggle(props) {
    return (
        <div className={styles.colorModeToggle}>
            <ColorModeToggle {...props} />
        </div>
    );
}
