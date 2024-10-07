import React, {useEffect, useState} from 'react';
import styles from './styles.module.scss';
import useBaseUrl from "@docusaurus/useBaseUrl";
import ThemedImage from '@theme/ThemedImage';

export default function GithubNavbarItem({ owner, repository, className }) {

    const [starsCount, setStarsCount] = useState('0');
    const [forksCount, setForksCount] = useState('0');
    const [showGithubBadgeInfo, setShowGithubBadgeInfo] = useState(true);

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

    const githubMarkImg = useBaseUrl(`img/github-mark.svg`);
    const githubLogoImg = useBaseUrl(`img/github.svg`);
    const starIcon = useBaseUrl(`img/star.svg`);
    const forkIcon = useBaseUrl(`img/code-branch.svg`);
    const githubMarkDarkImg = useBaseUrl(`img/github-mark-dark.svg`);
    const githubLogoDarkImg = useBaseUrl(`img/github-dark.svg`);
    const starDarkIcon = useBaseUrl(`img/star-dark.svg`);
    const forkDarkIcon = useBaseUrl(`img/code-branch-dark.svg`);

    return repository && (
        <a href={`https://github.com/${owner}/${repository}`} target="_blank"
           className={ className + ' ' + styles.githubBadge + ' ' + (showGithubBadgeInfo ? styles.githubBadgeNoErrors : '')}>
            <ThemedImage sources={{ dark: githubMarkDarkImg, light: githubMarkImg}} className={styles.githubMark}
                 alt="Github Invertocat Logo"/>
            { showGithubBadgeInfo && (<div className={styles.githubBadgeInfo}>
                <ThemedImage sources={{ dark: githubLogoDarkImg, light: githubLogoImg}} className={styles.githubLogo}
                     alt="Github Logo"/>
                <div className={styles.githubStats}>
                    <span>
                        <ThemedImage sources={{ dark: starDarkIcon, light: starIcon}} alt="Github Stargazers Count"/>
                        {starsCount}
                    </span>
                    <span>
                        <ThemedImage sources={{ dark: forkDarkIcon, light: forkIcon}} alt="Github Forks Count"/>
                        {forksCount}
                    </span>
                </div>
            </div>)}
        </a>
    );
}
