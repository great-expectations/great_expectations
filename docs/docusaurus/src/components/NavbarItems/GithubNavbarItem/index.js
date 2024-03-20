import React, {useEffect, useState} from 'react';
import styles from './styles.module.scss';
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function GithubNavbarItem({ owner, repository, className }) {

    const [starsCount, setStarsCount] = useState('0')
    const [forksCount, setForksCount] = useState('0')
    const [showGithubBadgeInfo, setShowGithubBadgeInfo] = useState(true)

    useEffect(() => {
        if(window.innerWidth > 996){
            fetch(`https://api.github.com/repos/${owner}/${repository}`)
                .then(response => response.json())
                .then(data => {
                    setStarsCount(formatCompactNumber(data.stargazers_count))
                    setForksCount(formatCompactNumber(data.forks_count))
                    setShowGithubBadgeInfo(true)
                }).catch( _ => {
                    setShowGithubBadgeInfo(false)
                })
        } else {
            setShowGithubBadgeInfo(false)
        }
    }, []);

    function formatCompactNumber(number) {
        const formatter = Intl.NumberFormat("en", { notation: "compact" });
        return formatter.format(number).toLowerCase();
    }

    const githubMarkImg = useBaseUrl(`img/github-mark.svg`);
    const githubLogoImg = useBaseUrl(`img/github.svg`);
    const starIcon = useBaseUrl(`img/star.svg`);
    const forkIcon = useBaseUrl(`img/code-branch.svg`);

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
