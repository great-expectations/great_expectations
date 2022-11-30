import React from 'react';
import clsx from 'clsx';
export default function FooterLayout({style, links, logo, copyright}) {
  return (
    <footer
      className={clsx('footer', {
        'footer--dark': style === 'dark',
      })}>
      <div 
        className="container container-fluid"
        style={{
          display: "flex",
          WebkitFlexDirection: "column",
          msFlexDirection: "column",
          flexDirection: "column",
          maxWidth: "1200px"
        }}
      >
        <div
          style={{
            WebkitFlexDirection: "row",
            msFlexDirection: "row",
            flexDirection: "row",
            boxSizing: "border-box",
            margin: "0",
            minWidth: "0",
            display:"flex",
            marginBottom: "32px",
            WebkitBoxPack: "justify",
            WebkitJustifyContent: "space-between",
            justifyContent: "space-between"
          }}
        >
          {(logo || copyright) && (
            <div 
              style={{
                boxSizing: "border-box",
                margin: "0",
                minWidth: "0",
                display: "flex",
                WebkitBoxPack: "start",
                msFlexPack: "start",
                WebkitJustifyContent: "flex-start",
                justifyContent: "flex-start",
                WebkitFlex: "1",
                msFlex: "1",
                flex: 1
              }}
              className="footer__bottom "
            >
              {logo && 
              <div style={{
                  boxSizing: "border-box",
                  margin: 0,
                  minWidth: 0,
                  maxWidth: "200px",
                  marginBottom: "50px",
                }}
              >
                {logo}
              </div>}
            </div>
          )}
          {links}
          </div>
          <div className="footer__bottom text--center">
            {copyright}
          </div>
      </div>
    </footer>
  );
}
