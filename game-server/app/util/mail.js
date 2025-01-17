module.exports = {
  HOST: "smtp.gmail.com",
  PORT: 25,
  AUTH_USER: "info@ig.games",
  AUTH_PASS: "sy!@0328",
  FROM_ADDRESS: "info@i8.games",
  FROM_USER: "info",

  tw: function (time, name, pass, url) {
    return (
      '<div style="margin:0 auto; width:600px">' +
      "<p>" +
      name +
      " 您好 ~</p>" +
      "<p>您收到這封電子郵件是因為您(或某人冒充您的名義)申請了一組新的密碼，假如不是您本人所申請，請不用理會這封電子郵件，但若您持續收到這類信件騷擾，請您儘快向管理人員聯繫。</p>" +
      '<p></p><p>新密碼： <font color="red">' +
      pass +
      "</font></p>" +
      "<p></p><p>若您要重設密碼，請於 " +
      time +
      " 前點擊下方連結進行密碼變更，謝謝。</p>" +
      "<p></p><p><a href='" +
      url +
      "'>臨時密碼登入</a></p>" +
      "</div>"
    )
  },
  cn: function (time, name, pass, url) {
    return (
      '<div style="margin:0 auto; width:600px">' +
      "<p>" +
      name +
      " 您好 ~</p>" +
      "<p>您收到这封电子邮件是因为您(或某人冒充您的名义)申请了一组新的密码，假如不是您本人所申请，请不用理会这封电子邮件，但若您持续收到这类信件骚扰，请您尽快向管理人员联系。</p>" +
      '<p></p><p>新密码： <font color="red">' +
      pass +
      "</font></p>" +
      "<p></p><p>若您要重设密码，请于 " +
      time +
      " 前点击下方连结进行密码变更，谢谢。</p>" +
      "<p></p><p><a href='" +
      url +
      "'>临时密码登入</a></p>" +
      "</div>"
    )
  },
  en: function (time, name, pass, url) {
    return (
      '<div style="margin:0 auto; width:600px">' +
      "<p>DEAR ~</p>" +
      "<p>You received this email because you (or someone impersonating you) applied for a new password.</p>" +
      "<p>If it is not your application, please ignore this email, but if you keep receiving Letter harassment,</p>" +
      "<p> please contact the management staff as soon as possible.</p>" +
      '<p></p><p>New Password: <font color="red">' +
      pass +
      "</font></p>" +
      "<p></p><p>If you want to reset your password, please click the link below to change your password before " +
      time +
      ".</p>" +
      "<p></p><p><a href='" +
      url +
      "'>TempPasswordUrl</a></p>" +
      "</div>"
    )
  },
  credit_to_mail: function (cid, userName, currency, dc, alertValue) {
    return (
      '<div style="margin:0 auto; width:600px">' +
      "<p>域名：" +
      dc +
      "</p>" +
      "<p>帳號：" +
      userName +
      "</p>" +
      "<p>編號：" +
      cid +
      "</p>" +
      "<p>幣別：" +
      currency +
      "</p>" +
      '<p></p><p>信用額度使用只剩 <font color="red">' +
      alertValue +
      "</font>%</p>" +
      "<p></p><p>-------------------------------------------------------------</p>" +
      "<p>域名：" +
      dc +
      "</p>" +
      "<p>帐号：" +
      userName +
      "</p>" +
      "<p>编号：" +
      cid +
      "</p>" +
      "<p>币别：" +
      currency +
      "</p>" +
      '<p></p><p>信用额度使用只剩 <font color="red">' +
      alertValue +
      "</font>%</p>" +
      "<p></p><p>-------------------------------------------------------------</p>" +
      "<p>DC：" +
      dc +
      "</p>" +
      "<p>Username：" +
      userName +
      "</p>" +
      "<p>ID：" +
      cid +
      "</p>" +
      "<p>Currency：" +
      currency +
      "</p>" +
      '<p></p><p>The credit quota has only left <font color="red">' +
      alertValue +
      "</font>%</p>" +
      "</div>"
    )
  },
  credit_to_reseller: function (cid, userName, currency, dc, alertValue) {
    return (
      '<div style="margin:0 auto; width:600px">' +
      "<p>域名：" +
      dc +
      "</p>" +
      "<p>帳號：" +
      userName +
      "</p>" +
      "<p>編號：" +
      cid +
      "</p>" +
      "<p>幣別：" +
      currency +
      "</p>" +
      '<p></p><p>信用額度使用只剩 <font color="red">' +
      alertValue +
      "</font>%，若需充值請盡快聯繫我們，謝謝。</p>" +
      "<p></p><p>-----------------------------------------------------------------------------------------</p>" +
      "<p>域名：" +
      dc +
      "</p>" +
      "<p>帐号：" +
      userName +
      "</p>" +
      "<p>编号：" +
      cid +
      "</p>" +
      "<p>币别：" +
      currency +
      "</p>" +
      '<p></p><p>信用额度使用只剩 <font color="red">' +
      alertValue +
      "</font>%，若需充值请尽快联系我们，谢谢。</p>" +
      "<p></p><p>-----------------------------------------------------------------------------------------</p>" +
      "<p>DC：" +
      dc +
      "</p>" +
      "<p>Username：" +
      userName +
      "</p>" +
      "<p>ID：" +
      cid +
      "</p>" +
      "<p>Currency：" +
      currency +
      "</p>" +
      '<p></p><p>The credit quota has only left <font color="red">' +
      alertValue +
      "</font>%, if you need to top up, please contact us as soon as possible, thank you.</p>" +
      "</div>"
    )
  },
}
