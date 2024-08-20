import React, {useEffect, useState} from 'react';
import styles from './styles.module.scss';
import useBaseUrl from "@docusaurus/useBaseUrl";
import {useColorMode} from '@docusaurus/theme-common';

export default function GithubNavbarItem({ owner, repository, className }) {

    const {colorMode, setColorMode} = useColorMode();
    const [starsCount, setStarsCount] = useState('0');
    const [forksCount, setForksCount] = useState('0');
    const [showGithubBadgeInfo, setShowGithubBadgeInfo] = useState(true);

    const [colorCode, setColorCode] = useState('');
    const [githubMarkImg, setGithubMarkImg] = useState(useBaseUrl(`img/github-mark.svg`));
    const [githubLogoImg, setGithubLogoImg] = useState(useBaseUrl(`img/github.svg`));
    const [starIcon, setStarIcon] = useState(useBaseUrl(`img/star.svg`));
    const [forkIcon, setForkIcon] = useState(useBaseUrl(`img/code-branch.svg`));

    useEffect(() => {
        setColorCode(colorMode === 'dark' ? '-dark' : '');
        setGithubMarkImg(useBaseUrl(`img/github-mark${colorCode}.svg`));
        setGithubLogoImg(useBaseUrl(`img/github${colorCode}.svg`));
        setStarIcon(useBaseUrl(`img/star${colorCode}.svg`));
        setForkIcon(useBaseUrl(`img/code-branch${colorCode}.svg`));
    }, [colorMode]);


    useEffect(() => {

        fetch(`https://api.github.com/repos/${owner}/${repository}`)
            .then(response => response.json())
            .then(data => {
                setStarsCount(formatCompactNumber(data.stargazers_count))
                setForksCount(formatCompactNumber(data.forks_count))
                setShowGithubBadgeInfo(true)
            }).catch( _ => {
            setShowGithubBadgeInfo(false)
        })
    }, []);

    function formatCompactNumber(number) {
        const formatter = Intl.NumberFormat("en", { notation: "compact" });
        return formatter.format(number).toLowerCase();
    }



    return repository && (
        <a href={`https://github.com/${owner}/${repository}`} target="_blank"
           className={ className + ' ' + styles.githubBadge + ' ' + (showGithubBadgeInfo ? styles.githubBadgeNoErrors : '')}>
            <img src={githubMarkImg} className={styles.githubMark}
                 alt="Github Invertocat Logo"/>
            { showGithubBadgeInfo && (<div className={styles.githubBadgeInfo}>
                <img src={githubLogoImg} className={styles.githubLogo}
                     alt="Github Logo"/>
                <div className={styles.githubStats}>
                    <span>
                        <img src={starIcon} alt="Github Stargazers Count"/>
                        {starsCount}
                    </span>
                    <span>
                        <img src={forkIcon} alt="Github Forks Count"/>
                        {forksCount}
                    </span>
                </div>
            </div>)}
        </a>
    );
}
