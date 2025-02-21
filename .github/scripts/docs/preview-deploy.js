const DOCS_SITE_REPO = {
  org: "edgedb",
  repo: "edgedb.com",
  ref: "master",
};

module.exports = async ({ github, context }) => {
  const { VERCEL_TOKEN, VERCEL_TEAM_ID } = process.env;

  if (!VERCEL_TOKEN || !VERCEL_TEAM_ID) {
    throw new Error(
      `cannot run docs preview deploy workflow, ` +
        `VERCEL_TOKEN or VERCEL_TEAM_ID secrets are missing`
    );
  }

  const prBranch = context.payload.pull_request.head.ref;
  const commitSHA = context.payload.pull_request.head.sha;
  const shortCommitSHA = commitSHA.slice(0, 8);

  const existingComments = (
    await github.rest.issues.listComments({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
    })
  ).data;

  const commentHeader = `### Docs preview deploy\n`;
  let commentMessage = commentHeader;

  let updateComment = existingComments.find(
    (c) =>
      c.performed_via_github_app?.slug === "github-actions" &&
      c.body?.startsWith(commentHeader)
  );

  let deploymentError = null;
  let deployment;
  try {
    deployment = await vercelFetch("https://api.vercel.com/v13/deployments", {
      name: "edgedb-docs",
      gitSource: {
        type: "github",
        ...DOCS_SITE_REPO,
      },
      projectSettings: {
        buildCommand: `EDGEDB_REPO_BRANCH=${prBranch} EDGEDB_REPO_SHA=${commitSHA} yarn vercel-build`,
      },
    });

    commentMessage += `\n🔄 Deploying docs preview for commit ${shortCommitSHA}:\n\n<https://${deployment.url}>`;
  } catch (e) {
    deploymentError = e;
    commentMessage += `\n❌ Failed to deploy docs preview for commit ${shortCommitSHA}:\n\n\`\`\`\n${e.message}\n\`\`\``;
  }

  commentMessage += `\n\n(Last updated: ${formatDatetime(new Date())})`;

  if (updateComment) {
    await github.rest.issues.updateComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      comment_id: updateComment.id,
      body: commentMessage,
    });
  } else {
    updateComment = (
      await github.rest.issues.createComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: context.issue.number,
        body: commentMessage,
      })
    ).data;
  }

  if (deploymentError) {
    throw new Error(`Docs preview deployment failed: ${e.message}`);
  }

  let i = 0;
  while (i < 40) {
    await sleep(15_000);
    i++;

    const status = (
      await vercelFetch(
        `https://api.vercel.com/v13/deployments/${deployment.id}`
      )
    ).status;

    const latestComment = await github.rest.issues.getComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      comment_id: updateComment.id,
    });

    if (!latestComment.data.body.includes(shortCommitSHA)) {
      console.log("Skipping further updates, new deployment has started");
      return;
    }

    if (status === "READY" || status === "ERROR" || status === "CANCELED") {
      await github.rest.issues.updateComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        comment_id: updateComment.id,
        body: `${commentHeader}${
          status === "READY"
            ? `\n✅ Successfully deployed docs preview for commit ${shortCommitSHA}:`
            : `\n❌ Docs preview deployment ${
                status === "CANCELED" ? "was canceled" : "failed"
              } for commit ${shortCommitSHA}:`
        }\n\n<https://${deployment.url}>\n\n(Last updated: ${formatDatetime(
          new Date()
        )})`,
      });
      if (status !== "READY") {
        throw new Error(
          `Docs preview deployment failed with status ${status}: https://${deployment.url}`
        );
      }
      return;
    }
  }

  await github.rest.issues.updateComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    comment_id: updateComment.id,
    body: `${commentHeader}
❌ Timed out waiting for deployment status to succeed or fail for commit ${shortCommitSHA}:\n\n<https://${
      deployment.url
    }>\n\n(Last updated: ${formatDatetime(new Date())})`,
  });
  throw new Error("Timed out waiting for deployment status to succeed or fail");
};

async function vercelFetch(url, body) {
  const { VERCEL_TOKEN, VERCEL_TEAM_ID } = process.env;
  const _url = new URL(url);
  url = `${_url.origin}${_url.pathname}?${new URLSearchParams({
    teamId: VERCEL_TEAM_ID,
  })}`;

  let res;
  try {
    res = await fetch(url, {
      body: body ? JSON.stringify(body) : undefined,
      headers: {
        Authorization: `Bearer ${VERCEL_TOKEN}`,
        "Content-Type": body ? "application/json" : undefined,
      },
      method: body ? "post" : "get",
    });
  } catch (e) {
    throw new Error(`vercel api request failed: ${e}`);
  }

  if (res.ok) {
    return await res.json();
  } else {
    let body;
    try {
      body = await res.text();
    } catch (e) {
      // ignore
    }
    throw new Error(
      `vercel api request failed: ${res.status} ${res.statusText}, ${body}`
    );
  }
}

function formatDatetime(date) {
  return date.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    second: "numeric",
    hourCycle: "h24",
    timeZoneName: "short",
  });
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
