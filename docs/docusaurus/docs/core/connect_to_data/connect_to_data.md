---
title: "Connect to data"
description: Learn how to connect to data using GX and how to organize that data into Batches for validation.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
      {frontMatter.description}
</OverviewCard>


<LinkCardGrid>
  
  <LinkCard 
      topIcon 
      label="SQL Data"
      description="Connect to data in SQL databases and organize it into Batches for validation."
      to="/core/connect_to_data/sql_data" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Filesystem Data"
      description="Connect to data stored as files in a folder hierarchy and organize it into Batches for validation."
      to="/core/connect_to_data/filesystem_data" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Dataframes"
      description="Connect to data in pandas or Spark Dataframes and organize it into Batches for validation."
      to="/core/connect_to_data/dataframes" 
      icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>