package bike.rapido.dq.sinks

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

class WebhookNotifier(webhookUrl: String) {
  private val client = HttpClients.createDefault()
  private def post(json: String): Int = { val post = new HttpPost(webhookUrl); post.addHeader("Content-Type", "application/json; charset=utf-8"); post.setEntity(new StringEntity(json, "UTF-8")); val resp = client.execute(post); try { val _ = Option(resp.getEntity).map(EntityUtils.toString); resp.getStatusLine.getStatusCode } finally resp.close() }
  def notify(text: String): Int = post(s"""{"text": ${JsonEncoding.q(text)}}""")
}

