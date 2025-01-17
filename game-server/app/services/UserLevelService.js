/**
 * 取得目前登入的玩家層級相關資訊
 *
 * cid若為sub已替換成其對應的hall或agent
 */
class UserLevelService {
  constructor({ session }) {
    if (session) {
      const level = session.get("level")
      this.currentUserLevel = level

      this.isAdmin = level === 1
      this.isHall = level === 2
      this.isAgent = level === 3

      this.isSub = session.get("isSub") === 1

      const sessionHallId = session.get("hallId")
      const sessionAgentId = session.get("agentId")
      const sessionCid = session.get("cid")

      this.cid = sessionCid

      if (this.isSub) {
        this.cid = this.isHall ? sessionHallId : sessionAgentId
      }
    }
  }

  /**
   * @typedef {Object} UserLevelData
   * @property {boolean} isAdmin
   * @property {boolean} isHall
   * @property {boolean} isAgent
   * @property {boolean} isSub
   * @property {string} cid
   * @property {number} currentUserLevel
   *
   * @returns UserLevelData
   */
  getUserLevelData() {
    return {
      isAdmin: this.isAdmin,
      isHall: this.isHall,
      isAgent: this.isAgent,
      isSub: this.isSub,
      cid: this.cid,
      currentUserLevel: this.currentUserLevel,
    }
  }
}

module.exports = UserLevelService
