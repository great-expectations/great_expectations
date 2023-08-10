import React from 'react';
import LinkItem from '@theme/Footer/LinkItem';
function ColumnLinkItem({item}) {
  return item.html ? (
    <div
      className="footer__item"
      // Developer provided the HTML, so assume it's safe.
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{__html: item.html}}
    />
  ) : (
    <div key={item.href ?? item.to} className="footer__item">
      <LinkItem item={item} />
    </div>
  );
}
function Column({column}) {
  return (
    <div 
      className="footer__col"
      style={{
        boxSizing: "border-box",
        margin: "0",
        minWidth: "0",
        display: "flex",
        WebkitFlexDirection: "column",
        msFlexDirection: "column",
        flexDirection: "column",
        WebkitAlignItems: "left",
        WebkitBoxAlign: "left",
        msFlexAlign: "left",
        alignItems: "left",
        WebkitBoxPack: "space-around",
        msFlexPack: "space-around",
        WebkitJustifyContent: "space-around",
        justifyContent: "space-around",
        width: "200px"
      }} 
    >
      <div className="footer__title">{column.title}</div>
      <div className="footer__items">
        <div
          style={{
            boxSizing: "border-box",
            margin: 0,
            minWidth: 0,
            display: "-webkit-box",
            display: "-webkit-flex",
            display: "-ms-flexbox",
            display: "flex",
            margin: 0,
            padding: 0,
            display: "-webkit-box",
            display: "-webkit-flex",
            display: "-ms-flexbox",
            display: "flex",
            WebkitFlexDirection: "column",
            MsFlexDirection: "column",
            flexDirection: "column",
            WebkitBoxAlign: "baseline",
            MsFlexAlign: "baseline",
            alignItems: "baseline",
          }}
        >
          {column.items.map((item, i) => (
            <ColumnLinkItem key={i} item={item} />
          ))}
        </div>
      </div>
    </div>
  );
}
export default function FooterLinksMultiColumn({columns}) {
  return (
    <div 
      style={{
        WebkitFlexDirection: "row",
        msFlexDirection: "row",
        flexDirection: "row",
        maxWidth: "50%",
        maxHeight: "100%",
        boxSizing: "border-box",
        margin: "0",
        minWidth: "0",
        display: "flex",
        WebkitBoxPack: "end",
        msFlexPack: "end",
        WebkitJustifyContent: "flex-end",
        justifyContent: "flex-end",
        marginBottom: "32px"
      }}
      className="footer__links">
      {columns.map((column, i) => (
        <Column key={i} column={column} />
      ))}
    </div>
  );
}
