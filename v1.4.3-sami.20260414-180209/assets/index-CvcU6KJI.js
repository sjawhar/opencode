function p(o={}){const{highlight:t,container:n}=o;return{async:!0,async walkTokens(e){var a;if(e.type!=="code"||typeof t!="function")return;const[r="text",...s]=((a=e.lang)==null?void 0:a.split(" "))??[],{text:c}=e,l=await t(c,r,s),i=n?n.replace("%l",String(r).toUpperCase()).replace("%s",l).replace("%t",c):l;Object.assign(e,{type:"html",block:!0,text:`${i}
`})}}}export{p as default};
