package com.wenzb.dto

case class Message(app_id:String, msg: Msg, msgtype:String, portal:String, touser:List[String])
