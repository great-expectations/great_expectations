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
            }).catch(error => {
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
    let forkIcon = useBaseUrl(`img/github-mark.svg`);

    return repository && (
        <a href={`https://github.com/${owner}/${repository}`} target="_blank" className={styles.github_badge}>
            <img src={githubMarkImg} className={styles.github_mark}
                 alt="Github Invertocat Logo"/>
            { showGithubBadgeInfo && (<div className={styles.github_badge_info}>
                <img src={githubLogoImg} className={styles.github_logo}
                     alt="Github Logo"/>
                <div className={styles.github_stats}>
                    <div>
                        <img src={starIcon} className={styles.github_mark}
                             alt="Github Stargazers Count"/>
                        {starsCount}
                    </div>
                    <div>
                        <img src={forkIcon} className={styles.github_mark}
                             alt="Github Forks Count"/>
                        {forksCount}
                    </div>
                </div>
            </div>)}
        </a>
    );
}
