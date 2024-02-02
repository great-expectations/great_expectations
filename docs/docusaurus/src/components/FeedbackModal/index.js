import React, {useState} from 'react';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function FeedbackModal(){

    const [isOpen, setIsOpen] = useState(false);

    const [formData, setFormData] = useState({
        name: '',
        workEmail: '',
        description: '',
        attachment: null
    });

    const handleChange = (e) => {
        const { name, value, files } = e.target;
        setFormData((prevData) => ({
            ...prevData,
            [name]: name === 'attachment' ? files[0] : value,
        }));
    };

    const thumbsUpImg = useBaseUrl(`img/thumbs_up_icon.svg`);
    const thumbsDownImg = useBaseUrl(`img/thumbs_down_icon.svg`);
    const closeImg = useBaseUrl(`img/close_icon.svg`);

    return <>
        <div className={styles.feedbackCard}>
            <div className={styles.feedbackCardTitle}>
                <h3>Was this helpful?</h3>
            </div>
            <div className={styles.feedbackCardBody}>
                <img src={thumbsUpImg} className={styles.feedbackIcon}
                     alt="Thumbs up icon"/>
                <img src={thumbsDownImg} className={styles.feedbackIcon}
                     alt="Thumbs down icon" onClick={() => setIsOpen(true)}/>
            </div>
        </div>

        {isOpen && <>
            <div className={styles.overlay} onClick={() => setIsOpen(false)}/>
            <div className={styles.modal}>
                <div className={styles.modalHeader}>
                    <h5 className={styles.modalHeaderTitle}>Tell Us More</h5>
                    <img src={closeImg} className={styles.modalHeaderCloseButton}
                         alt="Close icon" onClick={() => setIsOpen(false)}/>
                </div>

                <div className={styles.modalContent}>

                    Your opinion matters. Share your feedback here to help us improve
                    the quality of our documentation and ensure a better user experience.
                    Thank you for taking the time to share your experience.

                    <div className={styles.textInputs}>
                        <div className={styles.modalTextContainer}>
                            <label className={styles.modalTextLabel}>Name</label>
                            <input
                                type="text"
                                name="name"
                                className={styles.modalTextInput}
                                value={formData.name}
                                onChange={handleChange}
                                placeholder="Phillip"
                            />
                        </div>
                        <div className={styles.modalTextContainer}>
                            <label className={styles.modalTextLabel}>Work Email*</label>
                            <input
                                type="email"
                                name="workEmail"
                                className={styles.modalTextInput}
                                value={formData.workEmail}
                                onChange={handleChange}
                                placeholder="your_email@domain.com"
                            />
                        </div>
                    </div>
                    <div className={styles.modalTextContainer}>
                        <label className={styles.modalTextLabel}>Tell Us More</label>
                        <textarea
                            name="description"
                            value={formData.description}
                            className={styles.modalTextInput + ' ' + styles.modalTextareaInput}
                            onChange={handleChange}
                            placeholder="Provide as much detail as possible about the issue you
                            experienced or where improvement is needed. Detailed feedback helps
                            us better identify the problem and determine a solution."
                        />
                    </div>
                    <input
                        type="file"
                        name="attachment"
                        onChange={handleChange}
                    />
                    <button className={styles.submitButton} onClick={() => setIsOpen(false)}> Submit</button>
                </div>
            </div>
        </>}
    </>
}