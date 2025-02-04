module.exports = async ({ github, context }) => {
  const { VERCEL_TOKEN, VERCEL_TEAM_ID, GITHUB_SHA } = process.env;

  if (!VERCEL_TOKEN || !VERCEL_TEAM_ID) {
    throw new Error(
      `cannot run docs preview deploy workflow, ` +
        `VERCEL_TOKEN or VERCEL_TEAM_ID secrets are missing`
    );
  }

  console.log(context);
  return;

  const existingComments = (
    await github.rest.issues.listComments({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
    })
  ).data;

  let commentMessage = `# Docs preview deploy\n`;

  const updateComment = existingComments.find(
    (c) =>
      c.performed_via_github_app?.slug === "github-actions" &&
      c.body?.startsWith(commentMessage)
  );

  try {
    const deployment = await vercelFetch(
      "https://api.vercel.com/v13/deployments",
      {
        name: "edgedb-docs",
        gitSource: {
          type: "github",
          org: "edgedb",
          repo: "edgedb.com",
          ref: "docs-preview",
        },
        projectSettings: {
          buildCommand: `EDGEDB_REPO_BRANCH=${GITHUB_HEAD_REF} yarn vercel-build`,
        },
      }
    );

    commentMessage += `\n🔄 Deploying docs preview for commit ${GITHUB_SHA.slice(
      0,
      8
    )}:\n\n<https://${deployment.url}>`;
  } catch (e) {
    commentMessage += `\n❌ Failed to deploy docs preview for commit ${GITHUB_SHA.slice(
      0,
      8
    )}:\n\n\`\`\`\n${e.message}\n\`\`\``;
  }

  commentMessage += `\n\n(Last updated: ${formatDatetime(new Date())})`;

  if (updateComment) {
    github.rest.issues.updateComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      comment_id: updateComment.id,
      body: commentMessage,
    });
  } else {
    github.rest.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
      body: commentMessage,
    });
  }
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
      body: JSON.stringify(body),
      headers: {
        Authorization: `Bearer ${VERCEL_TOKEN}`,
        "Content-Type": "application/json",
      },
      method: "post",
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
