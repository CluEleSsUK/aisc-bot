const fs = require("fs")
const readline = require("readline")
const WebSubClient = require("@wedevelop/web-sub")
const KafkaAdapter = require("@wedevelop/web-sub-kafka-adapter")

const facebookApiKey = process.env.FACEBOOK_API_KEY
const webSubClient = new WebSubClient(new KafkaAdapter())
const channelsFilepath = "./channels.txt"

startPosting(facebookApiKey)

function startPosting(facebookApiKey) {
    const aiscPosterBot = new AiscPoster(facebookApiKey);

    forEachChannelIn(channelsFilepath, channel => {
        const channelSuffix = "https://api.twitch.tv/helix/streams?"

        whenTwitchChannelComesOnline(channelSuffix + channel, twitchOnlineMessage =>
            aiscPosterBot.postToAisc(channelIsLiveMessageFrom(twitchOnlineMessage))
                .then(() => console.log("Message posted to AISC"))
        )
    })
}

function forEachChannelIn(channelsFilepath, lineConsumer) {
    readline.createInterface({
        input: fs.createReadStream(channelsFilepath)
    }).on("line", lineConsumer)
}

function whenTwitchChannelComesOnline(channelTopic, handleChannelOnlineEvent) {
    webSubClient.subscribe(channelTopic, (err, twitchChannelEvent) => {
        if (err) {
            console.error("something didn't work", err)
            process.exit(1)
        }

        const offlineEvent = {data: []}
        if (twitchChannelEvent === offlineEvent) {
            return
        }

        handleChannelOnlineEvent(twitchChannelEvent)
    })
}

function AiscPoster(facebookApiKey) {
    this.facebookApiKey = facebookApiKey
    return this
}

AiscPoster.prototype.postToAisc = function (message) {
    const aiscGroupId = "165140136894580"

    return fetch(`https://graph.facebook.com/v3.3/${aiscGroupId}`, {
        method: "post",
        headers: {
            "Authorization": `Bearer ${this.facebookApiKey}`
        },
        body: message
    })
}

function channelIsLiveMessageFrom(twitchOnlineMessage) {
    const twitchUser = twitchOnlineMessage.user_name
    const title = twitchOnlineMessage.title
    return this.postToAisc(`${twitchUser} has just started streaming ${title}`)
}