# Anfrageformular mit Resend aktivieren

Die Website bleibt statisch auf GitHub Pages. Der Resend-Schlüssel liegt ausschließlich als Cloudflare-Worker-Secret und wird nie an den Browser ausgeliefert. Der Worker sendet neue Anfragen an `firasajam10@gmail.com` von `anfragen@kafitransporte.de`.

## Einmalige Einrichtung

1. In Resend die Domain `kafitransporte.de` hinzufügen und verifizieren. Die dort angezeigten DNS-Einträge bei Hostinger exakt übernehmen. Erst nach erfolgreicher Verifizierung kann der Worker von `anfragen@kafitransporte.de` senden.
2. In Cloudflare ein Konto einrichten und `workers.dev` für das Konto aktivieren. Für den Worker wird keine Änderung der bestehenden Nameserver oder der Website-DNS-Einträge benötigt.
3. In GitHub unter **Settings → Secrets and variables → Actions → Secrets** diese Repository-Secrets hinterlegen:
   - `CLOUDFLARE_API_TOKEN` mit Berechtigung zum Verwalten von Workers Scripts
   - `CLOUDFLARE_ACCOUNT_ID`
   - `RESEND_API_KEY` aus Resend
4. Die konfigurierte Worker-URL ist `https://kafi-worker.firasajam10.workers.dev/api/inquiries`. Der Pages-Build verwendet diese URL standardmäßig. Falls sie sich ändert, kann sie über **Settings → Secrets and variables → Actions → Variables** mit `KAFI_LEADS_API_URL` überschrieben werden.
5. Unter **Settings → Secrets and variables → Actions → Secrets** `CLOUDFLARE_API_TOKEN` (Workers Scripts: Edit) und `CLOUDFLARE_ACCOUNT_ID` hinterlegen. Der Worker wird unter dem Namen `kafi-worker` deployt und verwendet dadurch die obige URL. Der vorhandene Cloudflare-Worker liefert aktuell noch `Hello World!`; er ist also noch nicht als Formular-API eingerichtet.
6. `RESEND_API_KEY` als weiteres GitHub-Repository-Secret eintragen. Der API-Workflow deployt den Worker und setzt den Schlüssel anschließend sicher als Worker-Secret.
7. Den Code auf `master` pushen. **Deploy inquiry API** testet und deployt den Worker. Anschließend unter **Actions → Deploy to GitHub Pages → Run workflow** die Website mit der API-URL neu veröffentlichen.

## Sicherheit und Test

Der Worker beschränkt Browser-Aufrufe auf die KAFI-Domain, validiert alle Pflichtangaben, begrenzt die Request-Größe, escaped Inhalte in der E-Mail und verwirft ausgefüllte Honeypot-Felder. Der Resend-Key ist nicht Teil des öffentlichen Repositories oder des Pages-Builds. Die Worker-Tests laufen vor jedem API-Deployment.

Vor einer echten Anfrage den Preflight-Endpunkt prüfen: eine `OPTIONS`-Anfrage an `https://kafi-worker.firasajam10.workers.dev/api/inquiries` mit Origin `https://kafitransporte.de` muss Status `204` und `Access-Control-Allow-Origin: https://kafitransporte.de` liefern. Erst dann eine Testanfrage absenden und den Eingang in `firasajam10@gmail.com` prüfen. Der Versand funktioniert erst, wenn der Worker-Code deployed, die Resend-Domain verifiziert und `RESEND_API_KEY` als Worker-Secret gesetzt wurde; sonst zeigt das Formular eine Fehlermeldung und meldet keinen falschen Erfolg.