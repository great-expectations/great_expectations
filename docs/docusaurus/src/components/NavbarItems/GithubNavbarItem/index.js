import React, {useEffect, useState} from 'react';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";
export default function GithubNavbarItem({ owner, repository }) {

    const [starsCount, setStarsCount] = useState('0')
    const [forksCount, setForksCount] = useState('0')
    const [showGithubBadgeInfo, setShowGithubBadgeInfo] = useState(false)

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

    let githubMarkImg = useBaseUrl(`img/github-mark.svg`);
    let githubLogoImg = useBaseUrl(`img/github.svg`);
    let starIcon = useBaseUrl(`img/star.svg`);
    let forkIcon = useBaseUrl(`img/code-branch.svg`);

    return repository && (
        <a href={`https://github.com/${owner}/${repository}`} target="_blank" className={styles.githubBadge}>
            <img src={githubMarkImg} className={styles.githubMark}
                 alt="Github Invertocat Logo"/>
            { showGithubBadgeInfo && (<div className={styles.githubBadgeInfo}>
                <img src={githubLogoImg} className={styles.githubLogo}
                     alt="Github Logo"/>
                <div className={styles.githubStats}>
                    <div>
                        <img src={starIcon} alt="Github Stargazers Count"/>
                        {starsCount}
                    </div>
                    <div>
                        <img src={forkIcon} alt="Github Forks Count"/>
                        {forksCount}
                    </div>
                </div>
            </div>)}
        </a>
    );
}
